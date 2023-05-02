# -*- coding: utf-8 -*-
"""
    corsys.system
    ~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Optional, Iterator, Dict
from collections.abc import Sequence

import os
import re
import logging
import datetime as dt
import pandas as pd
from shutil import copytree, ignore_patterns

from .settings import Settings
from .tools import to_bool, to_date, ceil_date
from .io import Database, DatabaseException, DatabaseUnavailableException
from .weather import Weather, WeatherException, WeatherUnavailableException
from .configs import Configurations, ConfigurationUnavailableException
from .location import Location, LocationUnavailableException
from .cost import Cost, CostUnavailableException
from .cmpt import Component, Context

# noinspection SpellCheckingInspection
INVALID_CHARS = "'!@#$%^&?*;:,./\\|`´+~=- "

logger = logging.getLogger(__name__)


class System(Context):

    POWER_EL = 'el_power'
    POWER_EL_IMP = 'el_import_power'
    POWER_EL_EXP = 'el_export_power'
    POWER_TH = 'th_power'
    POWER_TH_HT = 'th_ht_power'
    POWER_TH_DOM = 'th_dom_power'

    ENERGY_EL = 'el_energy'
    ENERGY_EL_IMP = 'el_import_energy'
    ENERGY_EL_EXP = 'el_export_energy'
    ENERGY_TH = 'th_energy'
    ENERGY_TH_HT = 'th_ht_energy'
    ENERGY_TH_DOM = 'th_dom_energy'

    # noinspection PyProtectedMember, PyUnresolvedReferences
    @classmethod
    def read(cls, settings: Settings) -> Systems:
        systems = Systems()
        system_dirs = settings.dirs.encode()
        system_scan = to_bool(settings.get(Configurations.GENERAL, 'system_scan', fallback=False))
        system_flat = to_bool(settings.get(Configurations.GENERAL, 'system_flat', fallback=False))
        system_copy = to_bool(settings.get(Configurations.GENERAL, 'system_copy', fallback=False))

        if system_scan:
            if system_copy:
                cls._copy_configs(settings)

            for system_dir in os.scandir(settings.dirs.data):
                if os.path.isdir(system_dir.path):
                    if system_flat:
                        system_dirs['conf_dir'] = ''
                    system_dirs['data_dir'] = system_dir.path
                    systems._systems.append(cls._read(**system_dirs))
        else:
            systems._systems.append(cls._read(**system_dirs))

        return systems

    @classmethod
    def _copy_configs(cls, settings: Settings) -> bool:
        configs = Configurations(f"{cls.__name__.lower()}.cfg", **settings.dirs.encode())
        if configs.has_option('General', 'id'):
            system_id = cls._parse_id(configs[Configurations.GENERAL]['id'])
        elif configs.has_option(Configurations.GENERAL, 'name'):
            system_id = cls._parse_id(configs[Configurations.GENERAL]['name'])
            configs[Configurations.GENERAL]['id'] = system_id
        else:
            raise ValueError("Invalid configuration, missing specified system name")

        configs.dirs._data = os.path.join(configs.dirs.data, system_id)

        if os.path.isdir(configs.dirs.data):
            return False
        os.makedirs(configs.dirs.data, exist_ok=True)

        copytree(settings.dirs.conf,
                 configs.dirs.conf,
                 ignore=ignore_patterns('*.default.cfg',
                                        'evaluation*',
                                        'evaluations*',
                                        'settings*',
                                        'logging*'))
        return True

    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)

        if not configs.has_option(Configurations.GENERAL, 'name'):
            raise ValueError("Invalid configuration, missing specified system name")
        self._name = configs[Configurations.GENERAL]['name']

        if configs.has_option(Configurations.GENERAL, 'id'):
            self._id = self._parse_id(configs[Configurations.GENERAL]['id'])
        else:
            self._id = self._parse_id(configs[Configurations.GENERAL]['name'])

        if configs.has_section(Location.SECTION):
            self._location = self.__location__(configs)
        else:
            self._location = None

        if configs.has_section(Database.SECTION) and \
                configs.get(Database.SECTION, 'enabled', fallback='True').lower() == 'true' and \
                configs.get(Database.SECTION, 'enable', fallback='True').lower() == 'true':
            self._database = self.__database__(configs)
        else:
            self._database = None

        if configs.has_section(Cost.SECTION):
            self._cost = self.__cost__(configs)
        else:
            self._cost = None

        try:
            self._weather = self.__weather__(self.configs)

        except (WeatherUnavailableException, ConfigurationUnavailableException):
            self._weather = None
            logger.debug(f"System '{self.name}' has no weather configured")

    # noinspection PyMethodMayBeStatic
    def __location__(self, configs: Configurations) -> Location:
        return Location(configs.getfloat(Location.SECTION, 'latitude'),
                        configs.getfloat(Location.SECTION, 'longitude'),
                        timezone=configs.get(Location.SECTION, 'timezone', fallback='UTC'),
                        altitude=configs.getfloat(Location.SECTION, 'altitude', fallback=None),
                        country=configs.get(Location.SECTION, 'country', fallback=None),
                        state=configs.get(Location.SECTION, 'state', fallback=None))

    def __database__(self, configs: Configurations) -> Database:
        if configs.get(Database.SECTION, 'type').lower() == 'csv' and \
                configs.has_option(Database.SECTION, 'dir'):
            database_dir = configs.get(Database.SECTION, 'dir')
            if configs.has_option(Database.SECTION, 'central'):
                database_central = configs.getboolean(Database.SECTION, 'central')
                configs.remove_option(Database.SECTION, 'central')
            else:
                database_central = False
            if database_central:
                data_dir = configs.dirs.lib
            else:
                data_dir = configs.dirs.data

            if not os.path.isabs(database_dir):
                database_dir = os.path.join(data_dir, database_dir)
            if database_central:
                database_dir = os.path.join(
                    database_dir,
                    '{0:08.4f}'.format(float(self.location.latitude)).replace('.', '') + '_' +
                    '{0:08.4f}'.format(float(self.location.longitude)).replace('.', '')
                )
            configs.set(Database.SECTION, 'dir', database_dir)

        if not configs.has_option(Database.SECTION, 'timezone'):
            configs.set(Database.SECTION, 'timezone', self.location.timezone.zone)

        return Database.from_configs(configs)

    def __cost__(self, configs: Configurations) -> Cost:
        return Cost(**dict(configs.items(Cost.SECTION)))

    # noinspection PyUnusedLocal
    def __weather__(self, configs: Configurations) -> Weather:
        return Weather.read(self)

    def __activate__(self, components: Dict[str, Component]) -> None:
        super().__activate__(components)
        try:
            self.database.open()

        except DatabaseException as e:
            logger.debug(f"Unable to open database, as "+str(e))

        for component in components.values():
            component.activate()
        try:
            self.weather.activate()

        except WeatherException as e:
            logger.debug(f"Unable to activate weather, as "+str(e))

    def __build__(self,
                  start: str | pd.Timestamp | dt.datetime = None,
                  end:   str | pd.Timestamp | dt.datetime = None,
                  **kwargs) -> Optional[pd.DataFrame]:
        from scisys import build

        data = pd.DataFrame()
        try:
            database = self.database
            start = to_date(start, timezone=database.timezone)
            end = to_date(end, timezone=database.timezone)
            end = ceil_date(end)

            kwargs['start'] = start
            kwargs['end'] = end
            try:
                weather_data = self.weather.build(**kwargs)
                if weather_data is not None:
                    kwargs['weather'] = weather_data
                    data = pd.concat([weather_data, data], axis='columns')

            except (DatabaseUnavailableException, WeatherUnavailableException) as e:
                logger.debug(f"Unable to build weather, as "+str(e))

            system_data = build(self.configs, self.database, **kwargs)
            if system_data is not None:
                data = pd.concat([system_data, data], axis='columns')

        except DatabaseUnavailableException as e:
            logger.debug(f"Unable to build data, as "+str(e))

        for component in self.values():
            component_data = component.build(**kwargs)
            if component_data is not None:
                data = pd.concat([component_data, data], axis='columns')

        if data is None or data.empty:
            return None
        return data

    def __call__(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError

    @staticmethod
    def _parse_id(s: str) -> str:
        for c in INVALID_CHARS:
            s = s.replace(c, '_')
        return re.sub('[^\\w]+', '', s).lower()

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def location(self) -> Location:
        if not self._location:
            raise LocationUnavailableException(f"System \"{self.name}\" has no location configured")
        return self._location

    @property
    def database(self):
        if self._database is None:
            raise DatabaseUnavailableException(f"System \"{self.name}\" has no database configured")
        if not self._database.enabled:
            raise DatabaseUnavailableException(f"System \"{self.name}\" database is disabled")
        return self._database

    @property
    def cost(self):
        if self._cost is None:
            raise CostUnavailableException(f"System \"{self.name}\" has no costs configured")
        return self._cost

    @property
    def weather(self):
        if self._weather is None:
            raise WeatherUnavailableException(f"System \"{self.name}\" has no weather configured")
        return self._weather


class Systems(Sequence):

    def __init__(self, *systems: System) -> None:
        self._systems = list()
        self._systems.extend(systems)

    def __getitem__(self, index: int) -> System:
        return self._systems[index]

    def __iter__(self) -> Iterator[System]:
        return iter(self._systems)

    def __len__(self) -> int:
        return len(self._systems)

    def __call__(self, *args, **kwargs) -> None:
        for system in self:
            result = system(*args, **kwargs)
            try:
                system.database.write(result)

            except DatabaseUnavailableException as e:
                logger.debug(f"Skipping persisting results: {str(e)}")

    def build(self, **kwargs) -> None:
        for system in self:
            system.build(**kwargs)
