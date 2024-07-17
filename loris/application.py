# -*- coding: utf-8 -*-
"""
loris.application
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from threading import Event, Thread
from typing import Type

import pandas as pd
import pytz as tz
from loris import Channel, Settings, System
from loris.data.manager import DataManager
from loris.util import floor_date, to_timedelta


def load(name: str = "Loris", factory: Type[System] = System, **kwargs) -> Application:
    return Application(Settings(name, **kwargs)).setup(factory)


class Application(DataManager, Thread):
    _interval: int
    __interrupt: Event = Event()

    def __init__(self, settings: Settings, **kwargs) -> None:
        super().__init__(settings, name=settings.application, **kwargs)
        self.__interrupt = Event()
        self.__interrupt.set()

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return hash(id(self))

    def configure(self, settings: Settings) -> None:
        super().configure(settings)
        self._interval = settings.get_int("interval", default=1)

    def setup(self, factory: Type[System]) -> Application:
        self._logger.debug(f"Setting up {type(self).__name__}: {self.name}")
        self._do_load_systems(self.settings, factory)
        self._do_configure()
        return self

    # noinspection PyProtectedMember
    def _do_load_systems(self, settings: Settings, factory: Type[System]) -> None:
        systems = []
        systems_flat = settings.get_bool("system_flat", default=False)
        system_dirs = settings.dirs.encode()
        system_dirs["conf_dir"] = None
        if settings.get_bool("system_scan", default=False):
            if settings.get_bool("system_copy", default=False):
                factory.copy(settings)
            system_dirs["scan_dir"] = settings.dirs.data
            for system in factory.scan(self, **system_dirs, flat=systems_flat):
                systems.append(system)
        else:
            systems.append(factory.load(self, **system_dirs, flat=systems_flat))
        for system in systems:
            if system.id in self._components:
                self._components.get(system.id).configs.update(system.configs)
            else:
                self._components._add(system)

    # noinspection PyTypeChecker
    @property
    def settings(self) -> Settings:
        return self.configs

    @property
    def name(self) -> str:
        return self.settings.application

    def start(self) -> None:
        self._logger.info(f"Starting {type(self).__name__}: {self.name}")
        self.__interrupt.clear()
        super().start()

    def wait(self, **kwargs) -> None:
        self.join(**kwargs)

    def interrupt(self) -> None:
        self.__interrupt.set()

    # noinspection PyShadowingBuiltins
    def run(self, *args, **kwargs) -> None:
        self.read(*args, **kwargs)
        self._run(*args, **kwargs)

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

            def is_reading(channel: Channel, timestamp: pd.Timestamp) -> bool:
                freq = channel.freq
                if freq is None or not channel.has_connector():
                    return False
                return pd.isna(channel.connector.timestamp) or timestamp >= _next(channel.connector.timestamp, freq)

            now = pd.Timestamp.now(tz.UTC)
            channels = self.values().filter(lambda c: is_reading(c, now))

            self._logger.debug(f"Reading {len(channels)} channels of application: {self.name}")
            if len(channels) > 0:
                self.read(channels)
                self.notify(channels)

            self._run()
            self.log(channels)
        self.log()

    # noinspection PyUnresolvedReferences
    def _run(self, *args, **kwargs) -> None:
        for system in self.components.get_all(System):
            try:
                # TODO: Implement check if data was updated
                self._logger.debug(f"Running {type(system).__name__}: {system.name}")
                system.run(*args, **kwargs)

            except Exception as e:
                self._logger.warning(f"Error running system '{system.id}': ", str(e))


# noinspection PyShadowingBuiltins
def _next(time: pd.Timestamp, freq: str) -> pd.Timestamp:
    next = floor_date(time, freq=freq)
    while next <= time:
        next += to_timedelta(freq)
    return next
