# -*- coding: utf-8 -*-
"""
    corsys.weather.db
    ~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from typing import Optional

import os
import datetime as dt
import pandas as pd

from ..tools import to_date, to_bool
from ..configs import Configurations
from ..system import System
from ..io import Database
from .base import Weather, WeatherUnavailableException


class DatabaseWeather(Weather):

    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)

    def __activate__(self, system: System) -> None:
        super().__activate__(system)
        self._database = self.__database__(system, self.configs)
        self._database.open()

    # noinspection PyMethodMayBeStatic
    def __database__(self, system: System, configs: Configurations) -> Database:
        if not configs.has_section(Database.SECTION):
            raise WeatherUnavailableException("Weather database not configured")
        if not to_bool(configs.get(Database.SECTION, 'enabled', fallback='True')) or \
                not to_bool(configs.get(Database.SECTION, 'enable', fallback='True')):
            raise WeatherUnavailableException("Weather database not enabled")

        if configs.get(Database.SECTION, 'type').lower() == 'csv':
            if configs.has_option(Database.SECTION, 'dir'):
                database_dir = configs.get(Database.SECTION, 'dir')
                if configs.has_option(Database.SECTION, 'central'):
                    database_central = configs.getboolean(Database.SECTION, 'central')
                    configs.remove_option(Database.SECTION, 'central')
                else:
                    database_central = False
                if database_central:
                    if system is None:
                        raise ValueError('Invalid configuration, missing specified forecast id')

                    data_dir = configs.dirs.lib
                else:
                    data_dir = configs.dirs.data

                if not os.path.isabs(database_dir):
                    database_dir = os.path.join(data_dir, database_dir)
                if database_central:
                    database_dir = os.path.join(
                        database_dir,
                        '{0:06.2f}'.format(float(system.location.latitude)).replace('.', '') + '_' +
                        '{0:06.2f}'.format(float(system.location.longitude)).replace('.', '')
                    )
            else:
                database_dir = configs.dir.data

            configs.set(Database.SECTION, 'dir', database_dir)

            if not configs.has_option(Database.SECTION, 'timezone'):
                configs.set(Database.SECTION, 'timezone', system.location.timezone.zone)

        return Database.from_configs(configs)

    def __build__(self, **kwargs) -> Optional[pd.DataFrame]:
        from scisys import build
        return build(self.configs, self.database, location=self.system.location, **kwargs)

    @property
    def database(self):
        if not hasattr(self, '_database') or self._database is None:
            raise WeatherUnavailableException(f"Weather \"{self.system.location.name}\" has no database configured")
        if not self._database.enabled:
            raise WeatherUnavailableException(f"Weather \"{self.system.location.name}\" database is disabled")
        return self._database

    # noinspection PyShadowingBuiltins
    def get(self,
            start:  pd.Timestamp | dt.datetime | str = None,
            end:    pd.Timestamp | dt.datetime | str = None,
            format: str = '%d.%m.%Y',
            **kwargs) -> pd.DataFrame:

        start = to_date(start, timezone=self.system.location.timezone, format=format)
        end = to_date(end, timezone=self.system.location.timezone, format=format)

        return self.database.read(start=start, end=end, **kwargs)
