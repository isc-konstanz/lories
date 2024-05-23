# -*- coding: utf-8 -*-
"""
    loris.application
    ~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

import pytz as tz
import pandas as pd
import logging

from threading import Thread, Event
from loris import Settings, System, Channel
from loris.util import to_bool, to_timedelta, floor_date
from loris.data.manager import DataManager

logger = logging.getLogger(__name__)


# noinspection PyUnresolvedReferences
def load(name: str = 'Loris', **kwargs) -> Application:
    return Application.load(Settings(name, **kwargs))


class Application(DataManager, Thread):

    _interval: int
    _interrupt: Event = Event()

    # noinspection PyProtectedMember
    @classmethod
    def load(cls, settings: Settings):
        app = cls(settings)

        systems_flat = to_bool(settings.get('system_flat', default=False))
        system_dirs = settings.dirs.encode()
        system_dirs['conf_dir'] = None
        if to_bool(settings.get('system_scan', default=False)):
            if to_bool(settings.get('system_copy', default=False)):
                System.copy(settings)
            system_dirs['scan_dir'] = settings.dirs.data
            for system in System.scan(app, **system_dirs, flat=systems_flat):
                app.components._add(system)
        else:
            app.components._add(System.load(app, **system_dirs, flat=systems_flat))
        app.configure()
        return app

    def __init__(self, settings: Settings, **kwargs) -> None:
        super().__init__(settings, name=settings.application, **kwargs)
        self._interrupt = Event()
        self._interrupt.set()

    def __configure__(self, configs) -> None:
        super().__configure__(configs)
        self._interval = configs.get_int('interval', default=1)

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return hash(id(self))

    # noinspection PyTypeChecker
    @property
    def settings(self) -> Settings:
        return self.configs

    @property
    def name(self) -> str:
        return self.settings.application

    def start(self) -> None:
        logger.info(f"Starting {type(self).__name__}: {self.name}")
        self._interrupt.clear()
        super().start()

    def interrupt(self) -> None:
        self._interrupt.set()

    # noinspection PyShadowingBuiltins
    def run(self, *args, **kwargs) -> None:
        self.read(*args, **kwargs)
        self._run(*args, **kwargs)

        interval = f'{self._interval}s'
        while not self._interrupt.is_set():
            try:
                now = pd.Timestamp.now(tz.UTC)
                next = _next(now, interval)
                sleep = (next - now).total_seconds()
                logger.debug(f"Sleeping until next execution in {sleep} seconds: {next}")
                self._interrupt.wait(sleep)

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

            logger.debug(f"Reading {len(channels)} channels of application: {self.name}")
            if len(channels) > 0:
                self.read(channels)
                self.notify(channels)

            self._run()
            self.log(channels)
        self.log()

    def _run(self, *args, **kwargs) -> None:
        for system in self.components.get_all(System):
            try:
                # TODO: Implement check if data was updated
                logger.debug(f"Running {type(system).__name__}: {system.name}")
                system.run(*args, **kwargs)

            except Exception as e:
                logger.warning(f"Error running system \"{system.id}\":", str(e))


# noinspection PyShadowingBuiltins
def _next(time: pd.Timestamp, freq: str) -> pd.Timestamp:
    next = floor_date(time, freq=freq)
    while next <= time:
        next += to_timedelta(freq)
    return next
