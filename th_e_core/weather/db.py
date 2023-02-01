# -*- coding: utf-8 -*-
"""
    th-e-core.weather.db
    ~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations

import os
import datetime as dt
import pandas as pd

from ..tools import to_date, to_bool
from ..configs import Configurations
from ..system import System
from ..io import Database, DatabaseUnavailableException
from .wx import Weather


class DatabaseWeather(Weather):

    def __activate__(self, system: System, configs: Configurations) -> None:
        super().__activate__(system, configs)
        if not configs.has_section('Database'):
            raise DatabaseUnavailableException("Weather database not configured")
        if not to_bool(configs.get('Database', 'enabled', fallback='True')) or \
                not to_bool(configs.get('Database', 'enable', fallback='True')):
            raise DatabaseUnavailableException("Weather database not enabled")

        if configs.get('Database', 'type').lower() == 'csv':
            database_dir = configs.get('Database', 'dir')
            database_central = configs.getboolean('Database', 'central', fallback=False)
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
            configs.set('Database', 'dir', database_dir)

            if not configs.has_option('Database', 'timezone'):
                configs.set('Database', 'timezone', system.location.timezone.zone)

        self._database = Database.open(configs)

    # noinspection PyShadowingBuiltins
    def get(self,
            start:  pd.Timestamp | dt.datetime | str = None,
            end:    pd.Timestamp | dt.datetime | str = None,
            format: str = '%d.%m.%Y',
            **kwargs) -> pd.DataFrame:

        start = to_date(start, timezone=self.context.location.timezone)
        end = to_date(end, timezone=self.context.location.timezone)

        return self._database.read(start=start, end=end, **kwargs)
