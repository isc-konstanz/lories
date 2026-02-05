# -*- coding: utf-8 -*-
"""
lories.data.manager
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import signal
import time
from collections.abc import Callable
from concurrent import futures
from concurrent.futures import Future
from functools import partial
from threading import Event, Thread
from typing import Optional, Sequence

import pandas as pd
import pytz as tz
from lories._core import _Context, _DataManager  # noqa
from lories.components import Component, ComponentContext
from lories.connectors import Connector, ConnectorContext
from lories.connectors.tasks import LogTask, ReadTask
from lories.core.configs import Configurations
from lories.core.register import Registrator
from lories.core.typing import ChannelsArgument, Timestamp
from lories.data.channels import Channel, Channels
from lories.data.context import DataContext
from lories.data.converters import ConverterContext
from lories.data.databases import Database, Databases
from lories.data.listeners import ListenerContext
from lories.data.replication import Replication
from lories.data.retention import Retention
from lories.data.tasks import TaskContext, chain_filters
from lories.util import floor_date, to_bool, to_timedelta, validate_key

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


# noinspection PyProtectedMember
class DataManager(_DataManager, DataContext, TaskContext):
    _converters: ConverterContext
    _connectors: ConnectorContext
    _components: ComponentContext

    _listeners: ListenerContext

    __runner: Thread
    __interrupt: Event

    _interval: int

    def __init__(self, configs: Configurations, name: str, **kwargs) -> None:
        super().__init__(configs=configs, key=validate_key(name), name=name, **kwargs)
        signal.signal(signal.SIGINT, self.interrupt)
        signal.signal(signal.SIGTERM, self.terminate)
        self.__interrupt = Event()
        self.__interrupt.set()

        self._converters = ConverterContext(self)
        self._connectors = ConnectorContext(self)
        self._components = ComponentContext(self)
        self._listeners = ListenerContext(self)
        self.__runner = Thread(name=self.name, target=self.run, daemon=True)

    # noinspection PyArgumentList
    def __contains__(self, item: str | Channel | Connector | Component) -> bool:
        channels = _Context.__getattribute__(self, f"{_Context.__name__}__map")
        if isinstance(item, str):
            return item in channels.keys()
        if isinstance(item, Channel):
            return item in channels.values()
        if isinstance(item, Connector):
            return item in self._connectors.values()
        if isinstance(item, Component):
            return item in self._components.values()
        return False

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

    # noinspection PyShadowingBuiltins
    def activate(self, filter: Optional[Callable[[Registrator], bool]] = None) -> None:
        super().activate()
        self._connectors.connect(chain_filters(filter), self.channels)
        self._components.activate(chain_filters(filter))

    # noinspection PyShadowingBuiltins
    def deactivate(self, *_, filter: Optional[Callable[[Registrator], bool]] = None) -> None:
        super().deactivate()
        self._components.deactivate(chain_filters(filter))
        self._connectors.disconnect(chain_filters(filter))

    def interrupt(self, *_) -> None:
        self.__interrupt.set()
        super().interrupt()
        if self.__runner.is_alive():
            self.__runner.join()

    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        channels: Optional[ChannelsArgument] = None,
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

    def _filter_connectors(self, *filters: Optional[Callable[[Connector], bool]]) -> Sequence[Connector]:
        return self._connectors.filter(*filters)

    @property
    def components(self) -> ComponentContext:
        return self._components

    @property
    def listeners(self) -> ListenerContext:
        return self._listeners

    def __notify(
        self,
        channels: Optional[ChannelsArgument] = None,
        timeout: Optional[float] = None,
    ) -> None:
        channels = self._filter_by_args(channels)
        now = pd.Timestamp.now(tz.UTC)

        def _submit_listeners(_timeout: float) -> bool:
            _futures = []
            with self.listeners:
                for _listener in self.listeners.notify(*channels):
                    _future = self._submit(_listener, now)
                    _future.add_done_callback(self.__notify_callback)
                    _futures.append(_future)
            if len(_futures) > 0:
                futures.wait(_futures, timeout=_timeout)
                return True
            return False

        while _submit_listeners(timeout):
            if timeout is not None:
                timeout -= (pd.Timestamp.now(tz.UTC) - now).total_seconds()
                if timeout <= 0:
                    break

    # noinspection PyUnresolvedReferences
    def __notify_callback(self, future: Future) -> None:
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

        channels = self.channels.filter(lambda c: self.__is_reading(c, now))
        if len(channels) > 0:
            self.read(channels, inplace=True, **kwargs)

        interval = f"{self._interval}s"
        _sleep(interval)

        while not self.__interrupt.is_set():
            try:
                now = pd.Timestamp.now(tz.UTC)

                self.__read(now, timeout=self._interval / 4)

                self._connectors.reconnect(lambda c: c._is_reconnectable())
                self.__notify(timeout=self._interval / 4)
                self.__log()

                _sleep(interval, self.__interrupt.wait)

            except KeyboardInterrupt:
                self.interrupt()
                break

        self.__notify()
        self.__log()

    def has_logged(
        self,
        channels: Optional[ChannelsArgument] = None,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> bool:
        return self._has_logged(self._filter_by_args(channels), start=start, end=end, timeout=timeout)

    def read_logged(
        self,
        channels: Optional[ChannelsArgument] = None,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> pd.DataFrame:
        return self._read_logged(self._filter_by_args(channels), start=start, end=end, timeout=timeout)

    # noinspection PyTypeChecker
    def read(
        self,
        channels: Optional[ChannelsArgument] = None,
        timeout: Optional[float] = None,
        inplace: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        return self._read(self._filter_by_args(channels), timeout=timeout, inplace=inplace, **kwargs)

    # noinspection PyShadowingBuiltins, PyTypeChecker
    def __read(
        self,
        timestamp: Timestamp,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> None:
        channels = self.channels.filter(lambda c: self.__is_reading(c, timestamp))
        if len(channels) < 1:
            return
        self._logger.debug(f"Reading {len(channels)} channels of application: {self.name}")

        read_futures = []
        for id, connector in self.connectors.items():
            if not connector._is_connected():
                continue

            read_channels = channels.filter(lambda c: c.has_connector(id))
            if len(read_channels) == 0:
                continue

            read_task = ReadTask(connector, read_channels)
            read_future = self._submit(read_task, inplace=True, **kwargs)
            read_future.add_done_callback(partial(self._read_callback, read_task, inplace=True))
            read_futures.append(read_future)

            def update_timestamp(read_channel: Channel) -> None:
                read_channel.connector.timestamp = timestamp

            read_channels.apply(update_timestamp, inplace=True)

        futures.wait(read_futures, timeout=timeout)

    def __is_reading(self, channel: Channel, timestamp: pd.Timestamp) -> bool:
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

    # noinspection PyShadowingBuiltins, PyShadowingNames, PyTypeChecker
    def write(
        self,
        data: pd.DataFrame,
        channels: Optional[ChannelsArgument] = None,
        timeout: Optional[float] = None,
        inplace: bool = False,
    ) -> None:
        self._write(data, self._filter_by_args(channels), timeout=timeout, inplace=inplace)

    # noinspection PyShadowingBuiltins, PyTypeChecker
    def __log(
        self,
        channels: Optional[ChannelsArgument] = None,
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
            log_future = self._submit(log_task)
            log_futures[log_future] = log_task
            if not blocking:
                log_future.add_done_callback(partial(self._write_callback, log_task, inplace=False))

            def update_timestamp(channel: Channel) -> None:
                channel.logger.timestamp = channel.timestamp

            log_channels.apply(update_timestamp, inplace=True)

        if blocking:
            self._write_futures(log_futures, timeout, inplace=False)

    def rotate(
        self,
        channels: Optional[Channels] = None,
        full: bool = False,
        **kwargs,
    ) -> None:
        if channels is None:
            channels = self.channels

        defaults = self.configs.get_member(Retention.TYPE, defaults={})
        configs = Configurations(f"{Retention.TYPE}.conf", self.configs.dirs, defaults=defaults)
        configs._load(require=False)
        kwargs["full"] = configs.pop("full", default=full)

        databases = Databases(self, configs)
        databases.rotate(channels, **kwargs)

    def replicate(
        self,
        channels: Optional[Channels] = None,
        full: bool = False,
        force: bool = False,
        **kwargs,
    ) -> None:
        if channels is None:
            channels = self.channels.filter(lambda c: self.__is_replicating(c))

        defaults = self.configs.get_member(Replication.TYPE, defaults={})
        configs = Configurations(f"{Replication.TYPE}.conf", self.configs.dirs, defaults=defaults)
        configs._load(require=False)
        if not configs.enabled:
            self._logger.error(f"Unable to replicate for disabled configuration type '{Replication.TYPE}'")
            return
        kwargs["full"] = configs.pop("full", default=full)
        kwargs["force"] = configs.pop("force", default=force)
        kwargs.update({k: v for k, v in configs.items() if k not in configs.members})

        databases = Databases(self, configs)
        databases.replicate(channels, **kwargs)

    # noinspection PyMethodMayBeStatic
    def __is_replicating(self, channel: Channel, timestamp: Optional[Timestamp] = None) -> bool:
        replication = channel.get(Replication.TYPE, default=None)
        if not (
            replication is not None
            and "database" in replication
            and to_bool(replication.get("enabled", True))
            and channel.logger.enabled
            and isinstance(channel.logger._connector, Database)
        ):
            return False
        if timestamp is None:
            return True
        return timestamp <= floor_date(timestamp, freq=replication.get("freq", Replication.freq))


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
