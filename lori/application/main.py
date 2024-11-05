# -*- coding: utf-8 -*-
"""
lori.application.main
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import signal
from threading import Event, Thread
from typing import Collection, Optional, Type

import pandas as pd
import pytz as tz
from lori import Channel, Settings, System
from lori.application import Interface, InterfaceUnavailableException
from lori.components import ComponentContext
from lori.connectors import ConnectorContext
from lori.data.manager import DataManager
from lori.util import floor_date, to_timedelta


# noinspection PyProtectedMember
class Application(DataManager, Thread):
    TYPE: str = "app"
    SECTION: str = "application"
    SECTIONS: Collection[str] = [ComponentContext.SECTION, ConnectorContext.SECTION]

    _interface: Optional[Interface] = None

    _interval: int
    __interrupt: Event = Event()

    @classmethod
    def load(cls, name: str, factory: Type[System] = System, **kwargs) -> Application:
        settings = Settings(name, **kwargs)
        app = cls(settings)
        app.configure(settings, factory)
        return app

    def __init__(self, settings: Settings, **kwargs) -> None:
        super().__init__(settings, name=settings["name"], **kwargs)
        if not settings.has_section(Interface.SECTION):
            settings._add_section(Interface.SECTION, {"enabled": False})
        self._interface = Interface(self, settings.get_section(Interface.SECTION))
        self.__interrupt = Event()
        self.__interrupt.set()
        self._logger = logging.getLogger(self.key)

        signal.signal(signal.SIGINT, self.interrupt)
        signal.signal(signal.SIGTERM, self.terminate)

    # noinspection PyProtectedMember
    def configure(self, settings: Settings, factory: Type[System]) -> None:
        self._logger.debug(f"Setting up {type(self).__name__}: {self.name}")

        systems = []
        systems_flat = self.settings["systems"]["flat"]
        system_dirs = self.settings.dirs.to_dict()
        system_dirs["conf_dir"] = None
        if self.settings["systems"]["scan"]:
            if self.settings["systems"]["copy"]:
                factory.copy(self.settings)
            system_dirs["scan_dir"] = str(self.settings.dirs.data)
            for system in factory.scan(self, **system_dirs, flat=systems_flat):
                systems.append(system)
        else:
            systems.append(factory.load(self, **system_dirs, flat=systems_flat))
        for system in systems:
            if system.key in self._components:
                self._components.get(system.key).configs.update(system.configs)
            else:
                self._components._add(system)

        super().configure(settings)
        self._interval = settings.get_int("interval", default=1)
        if self._interface.is_enabled():
            self._interface.configure(settings.get_section(Interface.SECTION))

    @property
    def server(self) -> Interface:
        if self._interface is None:
            raise InterfaceUnavailableException(f"Application '{self.name}' has no server configured")
        return self._interface

    # noinspection PyTypeChecker
    @property
    def settings(self) -> Settings:
        return self.configs

    def start(self) -> None:
        self._logger.info(f"Starting {type(self).__name__}: {self.name}")
        self.__interrupt.clear()
        super().start()

        if self._interface is not None and self._interface.is_enabled():
            self._interface.start()

    def wait(self, **kwargs) -> None:
        if self.is_alive():
            self.join(**kwargs)

    def interrupt(self, *_) -> None:
        self.__interrupt.set()

    def terminate(self, *_) -> None:
        self.interrupt()
        self.wait()

    def deactivate(self) -> None:
        super().deactivate()
        self.terminate()

    # noinspection PyShadowingBuiltins, PyProtectedMember
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
                return

            for connector in self.connectors.filter(lambda c: c._is_reconnectable()):
                self.reconnect(connector)

            def is_reading(channel: Channel, timestamp: pd.Timestamp) -> bool:
                freq = channel.freq
                if (
                    freq is None
                    or not channel.has_connector()
                    or not self.connectors.get(channel.connector.id, False)
                    or not self.connectors.get(channel.connector.id).is_connected()
                ):
                    return False
                return pd.isna(channel.connector.timestamp) or timestamp >= _next(channel.connector.timestamp, freq)

            now = pd.Timestamp.now(tz.UTC)
            channels = self.channels.filter(lambda c: is_reading(c, now))

            if len(channels) > 0:
                self._logger.debug(f"Reading {len(channels)} channels of application: {self.name}")
                self.read(channels)
                self.notify(channels)

            self._run()
            self.log()
        self.log(force=True)

    # noinspection PyUnresolvedReferences
    def _run(self, *args, **kwargs) -> None:
        for system in self.components.get_all(System):
            try:
                # TODO: Implement check if data was updated
                self._logger.debug(f"Running {type(system).__name__}: {system.name}")
                system.run(*args, **kwargs)

            except Exception as e:
                self._logger.warning(f"Error running system '{system.key}': ", repr(e))


# noinspection PyShadowingBuiltins
def _next(time: pd.Timestamp, freq: str) -> pd.Timestamp:
    next = floor_date(time, freq=freq)
    while next <= time:
        next += to_timedelta(freq)
    return next
