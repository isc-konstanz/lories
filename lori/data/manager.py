# -*- coding: utf-8 -*-
"""
lori.data.manager
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import os
import signal
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError, as_completed
from functools import partial
from threading import Event, Thread
from typing import Any, Dict, Mapping, Optional, Type, overload

import pandas as pd
import pytz as tz
from lori.components.component import Component
from lori.components.context import ComponentContext
from lori.connectors import Connector, ConnectorException
from lori.connectors.context import ConnectorContext
from lori.connectors.tasks import CheckTask, ConnectTask, LogTask, ReadTask, WriteTask
from lori.converters.context import ConverterContext
from lori.core import Activator, Context, Entity
from lori.core.configs import ConfigurationException, Configurations
from lori.core.register import RegistratorContext
from lori.data.channels import Channel, ChannelConnector, ChannelConverter, Channels, ChannelState
from lori.data.context import DataContext
from lori.data.databases import Databases
from lori.data.listeners import ListenerContext
from lori.data.replication import Replicator
from lori.data.retention import Retention
from lori.data.typing import ChannelsType
from lori.typing import TimestampType
from lori.util import floor_date, parse_type, to_timedelta, validate_key

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


# noinspection PyProtectedMember
class DataManager(DataContext, Activator, Entity):
    _converters: ConverterContext
    _connectors: ConnectorContext
    _components: ComponentContext

    _listeners: ListenerContext

    _executor: ThreadPoolExecutor
    __runner: Thread
    __interrupt: Event

    _interval: int

    def __init__(self, configs: Configurations, name: str, **kwargs) -> None:
        super().__init__(configs=configs, key=validate_key(name), name=name, **kwargs)
        self.__interrupt = Event()
        self.__interrupt.set()

        self._converters = ConverterContext(self)
        self._connectors = ConnectorContext(self)
        self._components = ComponentContext(self)
        self._listeners = ListenerContext(self)
        self._executor = ThreadPoolExecutor(
            thread_name_prefix=self.name, max_workers=max(int((os.cpu_count() or 1) / 2), 1)
        )
        self.__runner = Thread(name=self.name, target=self.run)

        signal.signal(signal.SIGINT, self.interrupt)
        signal.signal(signal.SIGTERM, self.deactivate)

    # noinspection PyArgumentList
    def __contains__(self, item: str | Channel | Connector | Component) -> bool:
        channels = Context.__getattribute__(self, f"_{Context.__name__}__map")
        if isinstance(item, str):
            return item in channels.keys()
        if isinstance(item, Channel):
            return item in channels.values()
        if isinstance(item, Connector):
            return item in self._connectors.values()
        if isinstance(item, Component):
            return item in self._components.values()
        return False

    # noinspection PyShadowingBuiltins
    def _create(self, id: str, key: str, type: Type, **configs: Any) -> Channel:
        # noinspection PyShadowingBuiltins
        def build_args(
            registrator_context: RegistratorContext,
            registrator_type: str,
            name: Optional[str] = None,
        ) -> Dict[str, Any]:
            if name is None:
                name = registrator_type
            registrator_section = configs.pop(name, None)
            if registrator_section is None:
                return {registrator_type: None}
            if isinstance(registrator_section, str):
                registrator_section = {registrator_type: registrator_section}
            elif not isinstance(registrator_section, Mapping):
                raise ConfigurationException(f"Invalid channel {name} type: " + str(registrator_section))
            elif registrator_type not in registrator_section:
                return {registrator_type: None}

            registrator_id = registrator_section.pop(registrator_type)
            if registrator_id is not None and "." not in registrator_id:
                registrator_path = id.split(".")
                for i in reversed(range(1, len(registrator_path))):
                    _registrator_id = ".".join([*registrator_path[:i], registrator_id])
                    if _registrator_id in registrator_context.keys():
                        registrator_id = _registrator_id
                        break
            registrator = registrator_context.get(registrator_id, None) if registrator_id else None
            return {registrator_type: registrator, **registrator_section}

        if "converter" not in configs:
            converter = ChannelConverter(self._converters.get_by_dtype(parse_type(type)))
        else:
            converter = ChannelConverter(**build_args(self._converters, "converter"))
        connector = ChannelConnector(**build_args(self._connectors, "connector"))
        logger = ChannelConnector(**build_args(self._connectors, "connector", "logger"))

        return Channel(
            id=id, key=key, type=type, context=self, converter=converter, connector=connector, logger=logger, **configs
        )

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._interval = configs.get_int("interval", default=1)

    def _at_configure(self, configs: Configurations) -> None:
        super()._at_configure(configs)
        self._load(self, configs, sort=False)

        self._converters.load(configure=False, sort=False)
        self._converters.configure()

        self._connectors.load(configure=False, sort=False)
        self._connectors.configure()

        self._components.load(configure=False, sort=False)
        self._components.configure()

    def _on_configure(self, configs: Configurations) -> None:
        super()._on_configure(configs)
        self._converters.sort()
        self._connectors.sort()
        self._components.sort()
        self.sort()

    def activate(self) -> None:
        super().activate()
        self.connect(*self._connectors.filter(lambda c: c._is_connectable()))
        self._activate(*self._components.values())

    def _activate(self, *components: Component) -> None:
        for component in components:
            if not component.is_enabled():
                self._logger.debug(
                    f"Skipping activating disabled {type(component).__name__} '{component.name}': {component.id}"
                )
                continue

            self._logger.debug(f"Activating {type(component).__name__} '{component.name}': {component.id}")
            component.activate()

            self._logger.info(f"Activated {type(component).__name__} '{component.name}': {component.id}")

    def connect(
        self,
        *connectors: Connector,
        channels: Optional[Channels] = None,
        timeout: Optional[int] = None,
    ) -> None:
        connect_futures = {}
        for connector in connectors:
            if not connector.is_enabled():
                self._logger.debug(
                    f"Skipping connecting disabled {type(connector).__name__} '{connector.name}': {connector.id}"
                )
                continue

            if connector._is_connected():
                self._logger.debug(
                    f"Skipping already connected {type(connector).__name__} '{connector.name}': {connector.id}"
                )
                continue

            connect_task = self._connect(connector, channels)
            connect_future = self._executor.submit(connect_task)
            connect_futures[connect_future] = connect_task

        self._connect_futures(connect_futures, timeout)

    def _connect(self, connector: Connector, channels: Optional[Channels] = None) -> ConnectTask:
        self._logger.debug(f"Connecting {type(connector).__name__} '{connector.name}': {connector.id}")
        if channels is None:
            channels = self.channels.filter(lambda c: c.has_connector(connector.id))
            channels.update(self.channels.filter(lambda c: c.has_logger(connector.id)).apply(lambda c: c.from_logger()))

        return ConnectTask(connector, channels)

    def _connect_futures(
        self,
        futures: Dict[Future, ConnectTask],
        timeout: Optional[int] = None,
    ) -> None:
        try:
            for future in as_completed(futures, timeout=timeout):
                futures.pop(future)
                self._connect_callback(future)

        except TimeoutError:
            for future, task in futures.items():
                self._logger.warning(f"Timed out opening connector '{task.connector.id}' after {timeout} seconds")
                future.cancel()

    def _connect_callback(self, future: Future) -> None:
        try:
            connector = future.result()
            self._logger.info(f"Connected {type(connector).__name__} '{connector.name}': {connector.id}")

        except ConnectorException as e:
            self._logger.warning(f"Failed opening connector '{e.connector.id}': {str(e)}")
            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.exception(e)

    def reconnect(self, *connectors: Connector) -> None:
        for connector in connectors:
            if not connector.is_enabled():
                self._logger.debug(
                    f"Skipping reconnecting disabled {type(connector).__name__} '{connector.name}': {connector.id}"
                )
                continue

            if not connector._is_connected() and connector._connected:
                # Connection aborted and not yet disconnected properly
                self._disconnect(connector)
                continue

            connect_task = self._connect(connector)
            connect_future = self._executor.submit(connect_task)
            connect_future.add_done_callback(self._connect_callback)

    def disconnect(self, *connectors: Connector) -> None:
        for connector in reversed(connectors):
            if not connector._is_connected():
                self._logger.debug(
                    f"Skipping disconnecting unconnected {type(connector).__name__} '{connector.name}': {connector.id}"
                )
                continue
            self._disconnect(connector)

    def _disconnect(self, connector: Connector) -> None:
        try:
            self._logger.debug(f"Disconnecting {type(connector).__name__} '{connector.name}': {connector.id}")
            connector.set_channels(ChannelState.DISCONNECTING)
            connector.disconnect()

            self._logger.info(f"Disconnected {type(connector).__name__} '{connector.name}': {connector.id}")

        except Exception as e:
            self._logger.warning(f"Failed closing connector '{connector.id}': {str(e)}")
            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.exception(e)
        finally:
            connector.set_channels(ChannelState.DISCONNECTED)

    def deactivate(self, *_) -> None:
        self.interrupt()
        super().deactivate()
        self._deactivate(*self._components.values())
        self.disconnect(*self._connectors.values())

    def _deactivate(self, *components: Component) -> None:
        for component in reversed(list(components)):
            if not component.is_active():
                continue
            try:
                self._logger.debug(f"Deactivating {type(component).__name__} '{component.name}': {component.id}")
                component.deactivate()

                self._logger.info(f"Deactivated {type(component).__name__} '{component.name}': {component.id}")

            except Exception as e:
                self._logger.warning(f"Failed deactivating component '{component.id}': {str(e)}")
                if self._logger.getEffectiveLevel() <= logging.DEBUG:
                    self._logger.exception(e)

    def interrupt(self, *_) -> None:
        self.__interrupt.set()

        # FIXME: Add cancel_futures argument again, once Python >= 3.9 is a requirement
        self._executor.shutdown(wait=True)  # , cancel_futures=True)
        if self.__runner.is_alive():
            self.__runner.join()

    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        channels: Optional[ChannelsType] = None,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        self._listeners.register(function, self._filter_by_args(channels), how=how, unique=unique)

    @property
    def converters(self) -> ConverterContext:
        return self._converters

    @property
    def connectors(self) -> ConnectorContext:
        return self._connectors

    @property
    def components(self) -> ComponentContext:
        return self._components

    @property
    def listeners(self) -> ListenerContext:
        return self._listeners

    def notify(self, *channels: Channel) -> None:
        now = pd.Timestamp.now(tz.UTC)
        with self.listeners:
            for listener in self.listeners.notify(*channels):
                listener_future = self._executor.submit(listener, now)
                listener_future.add_done_callback(self._notify_callback)

    # noinspection PyUnresolvedReferences
    def _notify_callback(self, future: Future) -> None:
        exception = future.exception()
        if exception is not None:
            listener = exception.listener
            self._logger.warning(f"Failed notifying listener '{listener.id}': {str(exception)}")
            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.exception(exception)

    def start(self, wait: bool = True) -> None:
        self._logger.info(f"Starting {type(self).__name__}: {self.name}")
        self.__interrupt.clear()
        self.__runner.start()
        if wait:
            self.__runner.join()

    # noinspection PyShadowingBuiltins, PyProtectedMember
    def run(self, **kwargs) -> None:
        now = pd.Timestamp.now(tz.UTC)

        def is_reading(channel: Channel, timestamp: pd.Timestamp) -> bool:
            freq = channel.freq
            if (
                freq is None
                or not channel.has_connector()
                or not self.connectors.get(channel.connector.id, False)
                or not self.connectors.get(channel.connector.id).is_connected()
            ):
                return False
            if pd.isna(channel.connector.timestamp):
                return True
            next_reading = _next(freq, channel.connector.timestamp)
            return timestamp >= next_reading

        channels = self.channels.filter(lambda c: is_reading(c, now))
        if len(channels) > 0:
            self.read(channels, inplace=True, **kwargs)

        interval = f"{self._interval}s"
        _sleep(interval)

        while not self.__interrupt.is_set():
            try:
                now = pd.Timestamp.now(tz.UTC)

                channels = self.channels.filter(lambda c: is_reading(c, now))
                if len(channels) > 0:
                    self._logger.debug(f"Reading {len(channels)} channels of application: {self.name}")
                    self._run_read(channels)

                self.__interrupt.wait(self._interval / 2)
                self._listeners.wait(self._interval / 4, self.__interrupt.wait)
                self.log()

                for connector in self.connectors.filter(lambda c: c._is_reconnectable()):
                    self.reconnect(connector)

                _sleep(interval, self.__interrupt.wait)

            except KeyboardInterrupt:
                self.interrupt()
                break

        self._listeners.wait()
        self.log()

    # noinspection PyShadowingBuiltins
    def _run_read(self, channels: Optional[Channels] = None, **kwargs) -> None:
        channels = self._filter_by_args(channels)
        timestamp = pd.Timestamp.now(tz=tz.UTC)

        for id, connector in self.connectors.items():
            if not connector._is_connected():
                continue

            read_channels = channels.filter(lambda c: c.has_connector(id))
            if len(read_channels) == 0:
                continue

            read_task = ReadTask(connector, read_channels)
            read_future = self._executor.submit(read_task, **kwargs)
            read_future.add_done_callback(partial(self._read_callback, read_task, inplace=True))

            def update_timestamp(read_channel: Channel) -> None:
                read_channel.connector.timestamp = timestamp

            read_channels.apply(update_timestamp, inplace=True)

    @overload
    def has_logged(
        self,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> bool: ...

    @overload
    def has_logged(
        self,
        channels: ChannelsType,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> bool: ...

    # noinspection PyShadowingBuiltins
    def has_logged(
        self,
        channels: Optional[ChannelsType] = None,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
        timeout: Optional[int] = None,
    ) -> bool:
        channels = self._filter_by_args(channels)

        check_futures = {}
        for id, connector in self.connectors.items():
            if not connector._is_connected():
                continue

            def has_database(channel: Channel) -> bool:
                return channel.has_logger(id) and channel.logger.is_database()

            check_channels = channels.filter(has_database).apply(lambda c: c.from_logger())
            if len(check_channels) == 0:
                continue

            check_task = CheckTask(connector, check_channels)
            check_future = self._executor.submit(check_task, start=start, end=end)
            check_futures[check_future] = check_task

        check_results = []
        try:
            for check_future in as_completed(check_futures, timeout=timeout):
                check_task = check_futures.pop(check_future)
                try:
                    check_exists = check_future.result()
                    check_results.append(check_exists)

                except ConnectorException as e:
                    self._logger.warning(f"Failed checking connector '{check_task.connector.id}': {str(e)}")
                    if self._logger.getEffectiveLevel() <= logging.DEBUG:
                        self._logger.exception(e)

                    check_results.append(False)

        except TimeoutError:
            for check_future, check_task in check_futures.items():
                self._logger.warning(
                    f"Timed out checking connector '{check_task.connector.id}' after {timeout} seconds"
                )
                check_future.cancel()
                check_results.append(False)

        if len(check_results) == 0:
            return False
        return all(check_results)

    @overload
    def read_logged(
        self,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
        timeout: Optional[int] = None,
    ) -> pd.DataFrame: ...

    @overload
    def read_logged(
        self,
        channels: ChannelsType,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
        timeout: Optional[int] = None,
    ) -> pd.DataFrame: ...

    # noinspection PyShadowingBuiltins
    def read_logged(
        self,
        channels: Optional[Channels] = None,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
        timeout: Optional[int] = None,
    ) -> pd.DataFrame:
        channels = self._filter_by_args(channels)

        read_futures = {}
        for id, connector in self.connectors.items():
            if not connector._is_connected():
                continue

            def has_database(channel: Channel) -> bool:
                return channel.has_logger(id) and channel.logger.is_database()

            read_channels = channels.filter(has_database).apply(lambda c: c.from_logger())
            if len(read_channels) == 0:
                continue

            read_task = ReadTask(connector, read_channels)
            read_future = self._executor.submit(read_task, start=start, end=end)
            read_futures[read_future] = read_task

        return self._read_futures(read_futures, timeout)

    # noinspection PyShadowingBuiltins
    def read(
        self,
        channels: Optional[Channels] = None,
        timeout: Optional[float] = None,
        inplace: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        channels = self._filter_by_args(channels)

        read_futures = {}
        for id, connector in self.connectors.items():
            if not connector._is_connected():
                continue

            read_channels = channels.filter(lambda c: c.has_connector(id))
            if len(read_channels) == 0:
                continue

            read_task = ReadTask(connector, read_channels)
            read_future = self._executor.submit(read_task, **kwargs)
            read_futures[read_future] = read_task

        return self._read_futures(read_futures, timeout, inplace)

    def _read_futures(
        self,
        futures: Dict[Future, ReadTask],
        timeout: Optional[int] = None,
        inplace: bool = False,
    ) -> pd.DataFrame:
        results = []
        try:
            for future in as_completed(futures, timeout=timeout):
                task = futures.pop(future)
                data = self._read_callback(task, future, inplace)
                if data is not None:
                    results.append(data)

        except TimeoutError:
            for future, task in futures.items():
                self._logger.warning(f"Timed out reading connector '{task.connector.id}' after {timeout} seconds")
                future.cancel()
                if inplace:
                    channels = task.channels
                    channels.set_state(ChannelState.TIMEOUT)

        if len(results) == 0:
            return pd.DataFrame()
        results = sorted(results, key=lambda d: min(d.index))
        return pd.concat(results, axis="columns")

    def _read_callback(
        self,
        task: ReadTask,
        future: Future,
        inplace: bool = False,
    ) -> Optional[pd.DataFrame]:
        channels = task.channels
        try:
            data = future.result()
            data.dropna(axis="columns", how="all", inplace=True)
            if data is not None and not data.empty:
                if inplace:
                    channels.set_frame(data)
                return data
            elif inplace:
                channels.set_state(ChannelState.NOT_AVAILABLE)

        except ConnectorException as e:
            self._logger.warning(f"Failed reading connector '{task.connector.id}': {str(e)}")
            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.exception(e)

            channels.set_state(ChannelState.READ_ERROR)
        return None

    # noinspection PyShadowingBuiltins, PyShadowingNames, PyTypeChecker
    def write(
        self,
        data: pd.DataFrame,
        channels: Optional[Channels] = None,
        timeout: Optional[float] = None,
        inplace: bool = False,
    ) -> None:
        channels = self._filter_by_args(channels)

        write_futures = {}
        for id, connector in self.connectors.items():
            if not connector._is_connected():
                continue

            write_channels = channels.filter(lambda c: (c.has_connector(id) and c.id in data.columns))
            if len(write_channels) == 0:
                continue

            write_channels.set_frame(data)
            write_task = WriteTask(connector, write_channels)
            write_future = self._executor.submit(write_task)
            write_futures[write_future] = write_task

        self._write_futures(write_futures, timeout)

    def _write_futures(
        self,
        futures: Dict[Future, WriteTask | LogTask],
        timeout: Optional[int] = None,
        inplace: bool = False,
    ) -> None:
        try:
            for future in as_completed(futures, timeout=timeout):
                task = futures.pop(future)
                self._write_callback(task, future, inplace)

        except TimeoutError:
            for future, task in futures.items():
                self._logger.warning(f"Timed out writing connector '{task.connector.id}' after {timeout} seconds")
                future.cancel()
                if inplace:
                    channels = task.channels
                    channels.set_state(ChannelState.TIMEOUT)

    def _write_callback(
        self,
        task: WriteTask,
        future: Future,
        inplace: bool = False,
    ) -> None:
        channels = task.channels
        try:
            future.result()

        except ConnectorException as e:
            self._logger.warning(f"Failed writing connector '{task.connector.id}': {str(e)}")
            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.exception(e)
            if inplace:
                channels.set_state(ChannelState.WRITE_ERROR)

    # noinspection PyShadowingBuiltins, PyTypeChecker
    def log(
        self,
        channels: Optional[Channels] = None,
        timeout: Optional[float] = None,
        blocking: bool = False,
        force: bool = False,
    ) -> None:
        if channels is None:
            channels = self.channels

        log_futures = {}
        for id, connector in self.connectors.items():
            if not connector._is_connected():
                continue

            def has_update(channel: Channel) -> bool:
                if force:
                    return True
                if channel.freq is None:
                    return pd.isna(channel.logger.timestamp) or channel.timestamp > channel.logger.timestamp
                if pd.isna(channel.logger.timestamp):
                    logger_timestamp = floor_date(channel.timestamp, freq=channel.freq)
                    if logger_timestamp == channel.timestamp:
                        logger_timestamp -= channel.timedelta
                    channel.logger.timestamp = logger_timestamp

                return channel.timestamp >= channel.logger.timestamp + channel.timedelta

            log_channels = channels.filter(lambda c: (c.has_logger(id) and c.is_valid() and has_update(c)))
            if len(log_channels) == 0:
                continue

            log_task = LogTask(connector, log_channels)
            log_future = self._executor.submit(log_task)
            log_futures[log_future] = log_task
            if not blocking:
                log_future.add_done_callback(partial(self._write_callback, log_task, inplace=False))

            def update_timestamp(channel: Channel) -> None:
                channel.logger.timestamp = channel.timestamp

            log_channels.apply(update_timestamp, inplace=True)

        if blocking:
            self._write_futures(log_futures, timeout, inplace=False)

    def replicate(self, full: bool = False, force: bool = False, **kwargs) -> None:
        section = self.configs.get_section(Replicator.SECTION, defaults={})
        configs = Configurations(f"{Replicator.SECTION}.conf", self.configs.dirs, defaults=section)
        configs._load(require=False)
        if not configs.enabled:
            self._logger.error(f"Unable to replicate for disabled configuration section '{Replicator.SECTION}'")
            return
        kwargs["full"] = configs.pop("full", default=full)
        kwargs["force"] = configs.pop("force", default=force)
        kwargs.update({k: v for k, v in configs.items() if k not in configs.sections})

        databases = Databases(self, configs)
        databases.replicate(self.channels, **kwargs)

    def rotate(self, full: bool = False, **kwargs) -> None:
        section = self.configs.get_section(Retention.SECTION, defaults={})
        configs = Configurations(f"{Retention.SECTION}.conf", self.configs.dirs, defaults=section)
        configs._load(require=False)
        kwargs["full"] = configs.pop("full", default=full)

        databases = Databases(self, configs)
        databases.rotate(self.channels, **kwargs)


# noinspection PyShadowingBuiltins
def _sleep(freq: str, sleep: Callable = time.sleep) -> None:
    now = pd.Timestamp.now(tz.UTC)
    next = _next(freq, now)
    seconds = (next - now).total_seconds()
    sleep(seconds)


# noinspection PyShadowingBuiltins, PyShadowingNames
def _next(freq: str, now: Optional[pd.Timestamp] = None) -> pd.Timestamp:
    if now is None:
        now = pd.Timestamp.now(tz.UTC)
    next = floor_date(now, freq=freq)
    while next <= now:
        next += to_timedelta(freq)
    return next
