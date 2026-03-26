# -*- coding: utf-8 -*-
"""
lories.application.main
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import signal
import sys
import time
import traceback
from collections.abc import Callable
from concurrent import futures
from concurrent.futures import Future
from functools import partial
from threading import Event, Thread
from typing import Optional, Sequence, Type

import pandas as pd
import pytz as tz
from lories._core import _Application, _Context  # noqa
from lories.application import Interface, Settings
from lories.components import Component, ComponentContext, Weather
from lories.connectors import Connector, ConnectorContext, Database, DatabaseError
from lories.connectors.tasks import LogTask, ReadTask
from lories.core.configs import Configurations, ConfigurationUnavailableError
from lories.core.configs.parameters import Parameter, ParameterGroup
from lories.core.register import Registrator
from lories.core.typing import ChannelsArgument, Timestamp
from lories.data.channels import Channel, Channels
from lories.data.context import DataContext
from lories.data.converters import ConverterContext
from lories.data.databases import Databases
from lories.data.listeners import ListenerContext
from lories.data.processors import ProcessorContext
from lories.data.replication import Replication
from lories.data.retention import Retention
from lories.data.tasks import TaskContext, chain_filters
from lories.simulation import Results
from lories.system import System
from lories.util import floor_date, slice_range, to_bool, to_timedelta, validate_key

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


# noinspection PyProtectedMember
class Application(_Application, DataContext, TaskContext):
    _processors: ProcessorContext
    _converters: ConverterContext
    _connectors: ConnectorContext
    _components: ComponentContext
    _listeners: ListenerContext

    _interface: Optional[Interface] = None

    __runner: Thread
    __interrupt: Event

    _interval: int

    # Top-level Settings keys read directly by Application.
    _action = Parameter(key="action", type=str, required=False, default="start", desc="CLI action to perform")
    _interval = Parameter(
        key="interval", type=int, required=False, default=1, min=1, desc="Main loop interval in seconds"
    )
    _start = Parameter(key="start", type=str, required=False, desc="Simulation start timestamp")
    _end = Parameter(key="end", type=str, required=False, desc="Simulation end timestamp")
    _full = Parameter(key="full", type=bool, required=False, default=False, desc="Full retention/replication run")
    _force = Parameter(
        key="force", type=bool, required=False, default=False, desc="Force replication even when up-to-date"
    )
    _systems = ParameterGroup(
        key="systems",
        required=False,
        desc="System discovery settings",
        children=[
            Parameter(key="flat", type=bool, required=False, default=False, desc="Flat system layout"),
            Parameter(key="scan", type=bool, required=False, default=False, desc="Scan data dir for systems"),
            Parameter(key="copy", type=bool, required=False, default=False, desc="Copy settings before scanning"),
        ],
    )

    @classmethod
    def load(cls, name: str, factory: Type[System] = System, **kwargs) -> Application:
        settings = Settings(name, **kwargs)
        app = cls(settings)
        app.configure(settings, factory)
        return app

    def __init__(self, settings: Settings, **kwargs) -> None:
        super().__init__(configs=settings, key=validate_key(settings["name"]), name=settings["name"], **kwargs)
        signal.signal(signal.SIGINT, self.interrupt)
        signal.signal(signal.SIGTERM, self.terminate)
        self.__interrupt = Event()
        self.__interrupt.set()

        self._processors = ProcessorContext()
        self._converters = ConverterContext(self)
        self._connectors = ConnectorContext(self)
        self._components = ComponentContext(self)
        self._listeners = ListenerContext(self)

        if not settings.has_member(Interface.TYPE):
            settings._add_member(Interface.TYPE, {"enabled": False})

        # Check if the tasked action may be headless
        if settings.get("action").lower() == "start":
            self._interface = Interface(self, settings.get_member(Interface.TYPE))

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

    # noinspection PyTypeChecker, PyMethodOverriding
    def configure(self, settings: Settings, factory: Type[System] = System) -> None:
        super().configure(settings)
        self._logger.debug(f"Setting up {type(self).__name__}: {self.name}")
        self._interval = settings.get_int("interval", default=1)
        components = []
        try:
            system_dirs = settings.dirs.to_dict()
            system_dirs["conf_dir"] = None
            systems_configs = settings.get_member("systems")
            systems_flat = systems_configs.get_bool("flat")
            if systems_configs.get_bool("scan"):
                if systems_configs.get_bool("copy"):
                    factory.copy(self.settings)
                system_dirs["scan_dir"] = str(settings.dirs.data)
                components.extend(factory.scan(self._components, **system_dirs, flat=systems_flat))
            else:
                components.append(factory.load(self._components, **system_dirs, flat=systems_flat))

        except ConfigurationUnavailableError as e:
            self._logger.warning(str(e))

        if not self._components.has_type(System) and settings.dirs.data.is_default():
            components += self._components.load(configs_dir=settings.dirs.conf, configure=False, sort=False)

        if len(components) > 0:
            self._components.configure(components)
        if self._has_interface():
            self._interface.configure(settings.get_member(Interface.TYPE))

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

    # noinspection PyTypeChecker
    @property
    def settings(self) -> Settings:
        return self.configs

    @property
    def processors(self) -> ProcessorContext:
        return self._processors

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

    @property
    def interface(self) -> Interface:
        return self._interface

    def _has_interface(self) -> bool:
        return self._interface is not None and self._interface.is_enabled()

    def main(self) -> None:
        action = self.settings["action"]
        try:
            if action == "run":
                with self:
                    self.run(
                        start=self.settings.get_date("start", default=None),
                        end=self.settings.get_date("end", default=None),
                    )
            elif action == "start":
                with self:
                    self.start()

            elif action == "simulate":
                with self:
                    self.simulate(
                        start=self.settings.get_date("start", default=None),
                        end=self.settings.get_date("end", default=None),
                    )

            elif action == "rotate":
                self.rotate(full=self.settings.get_bool("full"))

            elif action == "replicate":
                self.replicate(full=self.settings.get_bool("full"), force=self.settings.get_bool("force"))

        except Exception as e:
            self._logger.warning(f"Error during '{action}': {str(e)}")
            self._logger.exception(e)
            exit(1)

    def start(self) -> None:
        self._logger.info(f"Starting {type(self).__name__}: {self.name}")
        self.__interrupt.clear()
        self.__runner.start()

        _has_interface = self._has_interface()
        if _has_interface:
            self._interface.start()
        else:
            self.__runner.join()

    # noinspection PyShadowingBuiltins
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
            or channel.is_listener()
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

    # noinspection PyUnresolvedReferences, PyShadowingBuiltins
    def simulate(
        self,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        **kwargs,
    ) -> None:
        simulation = self.settings.get_member("simulation", defaults={"data": {"include": True}})

        timezone = simulation.get("timezone", None)
        if start is None:
            start = simulation.get_date("start", default=None, timezone=timezone)
        if end is None:
            end = simulation.get_date("end", default=None, timezone=timezone)

        if (start is None or end is None) and self.components.has_type(Weather):
            weather = self.components.get_first(Weather).get()
            if not weather.empty:
                start = weather.index[0]
                end = weather.index[-1]

        if start is None or end is None or end < start:
            self._logger.error("Invalid settings missing specified simulation period")
            sys.exit(1)

        slice = simulation.get_bool("slice", default=True)
        freq = simulation.get("freq", default=None)

        database_id = simulation.get("database", default="results")
        database_configs = simulation.get_member("databases", defaults={})
        if database_id == "results" and "results" not in database_configs:
            database_configs["results"] = {
                "type": "tables",
                "file": ".results.h5",
                "compression_level": 9,
                "compression_lib": "zlib",
            }
        self.connectors.load(database_configs)

        database = self.connectors.get(database_id)
        if not isinstance(database, Database):
            raise DatabaseError(database, f"Invalid results cache type '{type(database)}'")

        error = False
        summary = []
        systems = self.components.get_all(System)
        for system in systems:
            self._logger.info(f"Starting simulation of system '{system.name}': {system.id}")
            if slice and freq is not None and start + to_timedelta(freq) < end:
                slices = slice_range(start, end, timezone=system.location.timezone, freq=freq)
            else:
                slices = [(start, end)]

            with Results(system, database, simulation.get_member("data"), total=len(slices)) as results:
                results.durations.start("Simulation")
                try:
                    for slice_start, slice_end in slices:
                        slice_prior = results.data.tail(1) if not results.data.empty else None
                        results.submit(
                            system.simulate,
                            slice_start,
                            slice_end,
                            slice_prior,
                            **kwargs,
                        )
                        results.progress.update()

                    results.durations.stop("Simulation")
                    self._logger.debug(
                        f"Finished simulation of system '{system.name}' in {results.durations['Simulation']} minutes"
                    )

                    self._logger.debug(f"Starting evaluation of system '{system.name}': {system.id}")
                    results.durations.start("Evaluation")
                    results_data = system.evaluate(results)
                    results.report()

                    # TODO: Call evaluations from configs
                    if "errors" in results_data.columns.get_level_values(0):
                        self._logger.debug(f"Starting evaluation of {len(results_data['errors'].columns)} errors")

                    results.durations.stop("Evaluation")
                    self._logger.debug(
                        f"Finished evaluation of system '{system.name}' in {results.durations['Evaluation']} minutes"
                    )

                    summary.append(results.to_frame())

                except Exception as e:
                    error = True
                    self._logger.error(f"Error simulating system {system.name}: {str(e)}")
                    self._logger.debug("%s: %s", type(e).__name__, traceback.format_exc())
                    results.durations.complete()
                    results.progress.complete(
                        status="error",
                        message=str(e),
                        error=type(e).__name__,
                        trace=traceback.format_exc(),
                    )

        if not error and len(summary) > 1:
            try:
                from lories.io import excel

                excel_file = str(self.configs.dirs.data.joinpath("summary.xlsx"))
                excel.write(excel_file, "Summary", pd.concat(summary, axis="index"))

            except ImportError:
                pass


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
