# -*- coding: utf-8 -*-
"""
lori.data.manager
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import logging
import os
import signal
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event, Thread
from typing import Any, Callable, Dict, Mapping, Optional, Type

import pandas as pd
import pytz as tz
from lori.components.component import Component
from lori.components.context import ComponentContext
from lori.connectors import Connector, ConnectorException
from lori.connectors.context import ConnectorContext
from lori.connectors.tasks import ConnectTask, LogTask, ReadTask, WriteTask
from lori.converters.context import ConverterContext
from lori.core import Activator, Context, Identifier, ResourceException
from lori.core.configs import ConfigurationException, Configurations, Configurator
from lori.core.register import RegistratorContext
from lori.data.channels import Channel, ChannelConnector, ChannelConverter, Channels, ChannelState
from lori.data.context import DataContext
from lori.data.databases import Databases
from lori.data.listeners import ListenerContext
from lori.data.replication import Replicator
from lori.data.retention import Retention
from lori.util import floor_date, get_variables, parse_type, to_timedelta, validate_key

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


# noinspection PyProtectedMember
class DataManager(DataContext, Activator, Identifier):
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

        self._converters = ConverterContext(self, configs)
        self._connectors = ConnectorContext(self, configs)
        self._components = ComponentContext(self, configs)
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
    def _new(self, id: str, key: str, type: Type, **configs: Any) -> Channel:
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
            if "." not in registrator_id:
                registrator_path = id.split(".")
                for i in reversed(range(1, len(registrator_path))):
                    _registrator_id = ".".join([*registrator_path[:i], registrator_id])
                    if _registrator_id in registrator_context.keys():
                        registrator_id = _registrator_id
                        break
            return {registrator_type: registrator_context.get(registrator_id, None), **registrator_section}

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
        self._load(self, configs)

        self._configure(self._converters)
        self._configure(self._connectors)
        self._configure(self._components)

        self._configure(*get_variables(self._components.values(), include=ComponentContext))
        self._configure(*get_variables(self._components.values(), exclude=ComponentContext))
        self._configure(*get_variables(self._connectors.values(), exclude=Component))
        self._configure(*get_variables(self._converters.values(), exclude=(Component, Connector)))

        self._converters.sort()
        self._components.sort()
        self._connectors.sort()

    def _configure(self, *configurators: Configurator) -> None:
        for configurator in configurators:
            if not configurator.is_enabled():
                self._logger.debug(
                    f"Skipping configuring disabled {type(configurator).__name__}: " f"{configurator.configs.name}"
                )
                continue
            self._logger.debug(f"Configuring {type(self).__name__}: {configurator.configs.path}")
            configurations = configurator.configs
            configurator.configure(configurations)

            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug(f"Configured {configurator}")

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

            self._logger.info(f"Activating {type(component).__name__} '{component.name}': {component.id}")
            component.activate()

            self._logger.debug(f"Activated {type(component).__name__} '{component.name}': {component.id}")

    def connect(self, *connectors: Connector, channels: Optional[Channels] = None) -> None:
        connect_futures = []
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
            connect_futures.append(self._connect(connector, channels))
        for connect_future in futures.as_completed(connect_futures):
            self._connect_callback(connect_future)
        # for connect_future in connect_futures:
        #     connect_future.add_done_callback(self._connect_callback)

    def _connect(self, connector: Connector, channels: Optional[Channels] = None) -> Future:
        self._logger.info(f"Connecting {type(connector).__name__} '{connector.name}': {connector.id}")
        if channels is None:
            channels = self.channels.filter(lambda c: c.has_connector(connector.id))
            channels.update(self.channels.filter(lambda c: c.has_logger(connector.id)).apply(lambda c: c.from_logger()))

        return self._executor.submit(ConnectTask(connector, channels))

    def _connect_callback(self, future: Future) -> None:
        try:
            connector = future.result().connector
            self._logger.debug(f"Connected {type(connector).__name__} '{connector.name}': {connector.id}")

        except ConnectorException as e:
            self._logger.warning(f"Error opening connector '{e.connector.id}': {str(e)}")
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.exception(e)

    def reconnect(self, *connectors: Connector) -> None:
        connect_futures = []
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
            connect_futures.append(self._connect(connector))
        for connect_future in futures.as_completed(connect_futures):
            self._connect_callback(connect_future)
        # for connect_future in connect_futures:
        #     connect_future.add_done_callback(self._connect_callback)

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
            self._logger.info(f"Disconnecting {type(connector).__name__} '{connector.name}': {connector.id}")
            connector.set_channels(ChannelState.DISCONNECTING)
            connector.disconnect()

            self._logger.debug(f"Disconnected {type(connector).__name__} '{connector.name}': {connector.id}")

        except Exception as e:
            self._logger.warning(f"Error closing connector '{connector.id}': {str(e)}")
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.exception(e)
        finally:
            connector.set_channels(ChannelState.DISCONNECTED)

    def deactivate(self, *_) -> None:
        super().deactivate()
        self._deactivate(*self._components.values())
        self.disconnect(*self._connectors.values())
        self.interrupt()

    def _deactivate(self, *components: Component) -> None:
        for component in reversed(list(components)):
            if not component.is_active():
                continue
            try:
                self._logger.info(f"Deactivating {type(component).__name__} '{component.name}': {component.id}")
                component.deactivate()

                self._logger.debug(f"Deactivated {type(component).__name__} '{component.name}': {component.id}")

            except Exception as e:
                self._logger.warning(f"Error deactivating component '{component.id}': {str(e)}")
                if self._logger.isEnabledFor(logging.DEBUG):
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
        *channels: Channel | str,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        _channels = []
        for channel in channels:
            if isinstance(channel, str):
                if channel in self:
                    channel = self[channel]
            if not isinstance(channel, Channel):
                raise ResourceException(f"Unable to register to '{type(channel)}' channel: {channel}")
            _channels.append(channel)
        self._listeners.register(function, Channels(_channels), how=how, unique=unique)

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
            self._logger.warning(f"Error notifying listener '{listener.id}': {str(exception)}")
            if self._logger.isEnabledFor(logging.DEBUG):
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
            next_reading = _next(channel.connector.timestamp, freq)
            return timestamp >= next_reading

        channels = self.channels.filter(lambda c: is_reading(c, now))
        if len(channels) > 0:
            self.read(channels, **kwargs)

        interval = f"{self._interval}s"
        while not self.__interrupt.is_set():
            try:
                now = pd.Timestamp.now(tz.UTC)
                next = _next(now, interval)
                sleep = (next - now).total_seconds()
                self._logger.debug(f"Sleeping until next execution in {sleep} seconds: {next}")
                self.__interrupt.wait(sleep)

            except KeyboardInterrupt:
                self.interrupt()
                break

            for connector in self.connectors.filter(lambda c: c._is_reconnectable()):
                self.reconnect(connector)

            now = pd.Timestamp.now(tz.UTC)

            channels = self.channels.filter(lambda c: is_reading(c, now))
            if len(channels) > 0:
                self._logger.debug(f"Reading {len(channels)} channels of application: {self.name}")
                self.read(channels)

            self.log()

        self.listeners.wait()
        self.log()

    def replicate(self, full: bool = False, **kwargs) -> None:
        section = self.configs.get_section(Replicator.SECTION, defaults={})
        configs = Configurations(f"{Replicator.SECTION}.conf", self.configs.dirs, defaults=section)
        configs._load(require=False)
        if not configs.enabled:
            self._logger.error(f"Unable to replicate for disabled configuration section '{Replicator.SECTION}'")
            return
        kwargs["full"] = configs.pop("full", default=full)
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

    # noinspection PyShadowingBuiltins, PyTypeChecker
    def read(
        self,
        channels: Optional[Channels] = None,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame:
        time = pd.Timestamp.now(tz=tz.UTC)
        if channels is None:
            channels = self.channels

        read_tasks = {}
        read_futures = []
        for id, connector in self.connectors.items():
            if not connector._is_connected():
                continue

            read_channels = channels.filter(lambda c: c.has_connector(id))
            if len(read_channels) == 0:
                continue
            read_task = ReadTask(connector, read_channels)
            read_tasks[id] = read_task
            read_futures.append(self._executor.submit(read_task, start=start, end=end))

        read_data = []
        for read_future in futures.as_completed(read_futures):
            try:
                read_channels = read_future.result().channels
                read_data.append(read_channels.to_frame(unique=True))

                def update_connector(read_channel: Channel) -> Channel:
                    read_channel.connector.timestamp = time
                    return read_channel

                read_channels.apply(update_connector, inplace=True)

            except ConnectorException as e:
                self._logger.warning(f"Error reading connector '{e.connector.id}': {str(e)}")
                if self._logger.isEnabledFor(logging.DEBUG):
                    self._logger.exception(e)

                def update_state(channel: Channel) -> Channel:
                    channel.state = ChannelState.UNKNOWN_ERROR
                    return channel

                read_task = read_tasks[e.connector.id]
                read_task.channels.apply(update_state, inplace=True)

        if len(read_data) > 0:
            return pd.concat(read_data, axis="columns")
        return pd.DataFrame()

    # noinspection PyShadowingBuiltins
    def write(self, data: pd.DataFrame, channels: Optional[Channels] = None) -> None:
        time = pd.Timestamp.now(tz=tz.UTC)
        if channels is None:
            channels = self.channels

        write_tasks = {}
        write_futures = []
        for id, connector in self.connectors.items():
            if not connector._is_connected():
                continue

            write_channels = channels.filter(lambda c: (c.has_connector(id) and c.id in data.columns))
            if len(write_channels) == 0:
                continue

            write_channels.set_frame(data)
            write_task = WriteTask(connector, write_channels)
            write_tasks[id] = write_task
            write_futures.append(self._executor.submit(write_task))

        for write_future in futures.as_completed(write_futures):
            try:
                write_task = write_future.result()

                # noinspection PyShadowingNames
                def update_connector(channel: Channel) -> Channel:
                    channel.connector.timestamp = time
                    return channel

                write_task.channels.apply(update_connector, inplace=True)

            except ConnectorException as e:
                self._logger.warning(f"Error writing connector '{e.connector.id}': {str(e)}")
                if self._logger.isEnabledFor(logging.DEBUG):
                    self._logger.exception(e)

                # noinspection PyShadowingNames
                def update_state(channel: Channel) -> Channel:
                    channel.state = ChannelState.UNKNOWN_ERROR
                    return channel

                write_task = write_tasks[e.connector.id]
                write_task.channels.apply(update_state, inplace=True)

    # noinspection PyShadowingBuiltins
    def log(self, channels: Optional[Channels] = None, force: bool = False) -> None:
        if channels is None:
            channels = self.channels

        log_tasks = {}
        log_futures = []
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
            log_tasks[id] = log_task
            log_futures.append(self._executor.submit(log_task))

        for write_future in futures.as_completed(log_futures):
            try:
                log_task = write_future.result()

                def update_logger(channel: Channel) -> Channel:
                    channel.logger.timestamp = channel.timestamp
                    return channel

                log_task.channels.apply(update_logger, inplace=True)

            except ConnectorException as e:
                self._logger.warning(f"Error logging connector '{e.connector.id}': {str(e)}")
                if self._logger.isEnabledFor(logging.DEBUG):
                    self._logger.exception(e)


# noinspection PyShadowingBuiltins
def _next(time: pd.Timestamp, freq: str) -> pd.Timestamp:
    next = floor_date(time, freq=freq)
    while next <= time:
        next += to_timedelta(freq)
    return next
