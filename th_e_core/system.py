# -*- coding: utf-8 -*-
"""
    th-e-core.system
    ~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Dict, Iterator
from collections.abc import Sequence

import os
import re
import logging
import pandas as pd
from shutil import copytree, ignore_patterns

from .settings import Settings
from .tools import to_bool
from .io import Database, DatabaseUnavailableException
from .location import Location, LocationUnavailableException
from .configs import Configurations, ConfigurationUnavailableException
from .weather import Weather, WeatherUnavailableException
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

    # noinspection PyProtectedMember
    @classmethod
    def read(cls, settings: Settings) -> Systems:
        systems = Systems()
        system_dirs = settings.dirs.encode()
        system_scan = to_bool(settings.get(Configurations.GENERAL, 'system_scan', fallback=False))
        system_copy = to_bool(settings.get(Configurations.GENERAL, 'system_copy', fallback=False))

        if system_scan:
            if system_copy:
                cls._copy_configs(settings)

            for system_dir in os.scandir(settings.dirs.data):
                if os.path.isdir(system_dir.path):
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

    def __init__(self, configs: Configurations, **kwargs) -> None:
        super().__init__(configs, **kwargs)
        self.__activate__(self._components, configs)

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

    def __activate__(self, components: Dict[str, Component], configs: Configurations) -> None:
        try:
            self._weather = self.__weather__(configs)

        except ConfigurationUnavailableException:
            self._weather = None
            logger.debug(f"System '{self.name}' has no weather configured")

    # noinspection PyUnusedLocal
    def __weather__(self, configs: Configurations) -> Weather:
        return Weather.read(self)

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
            database_central = configs.getboolean(Database.SECTION, 'central', fallback=False)
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

        return Database.open(configs)

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
    def database(self):
        if self._database is None:
            raise DatabaseUnavailableException(f"System \"{self.name}\" has no database configured")
        if not self._database.enabled:
            raise DatabaseUnavailableException(f"System \"{self.name}\" database is disabled")

        return self._database

    @property
    def location(self) -> Location:
        if not self._location:
            raise LocationUnavailableException(f"System \"{self.name}\" has no location configured")

        return self._location

    @property
    def weather(self):
        if self._weather is None:
            raise WeatherUnavailableException(f"System \"{self.name}\" has no weather configured")

        return self._weather

    def build(self, **kwargs) -> pd.DataFrame:
        return self.__build__(**kwargs)

    def __build__(self, **kwargs) -> pd.DataFrame:
        from th_e_data import build
        data = pd.DataFrame()
        try:
            weather = self.weather.build(**kwargs)
            if weather is not None:
                kwargs['weather'] = weather
                data = pd.concat([data, weather], axis=1)

        except WeatherUnavailableException as e:
            logger.debug(f"Unable to build weather, as "+str(e))
        try:
            data = pd.concat([data, build(self.configs, self.database, **kwargs)], axis=1)

        except DatabaseUnavailableException as e:
            logger.debug(f"Unable to build data, as "+str(e))

        return data

    def __call__(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError


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

    def build(self, **kwargs) -> None:
        for system in self:
            system.build(**kwargs)

    def __call__(self, *args, **kwargs) -> None:
        for system in self:
            result = system(*args, **kwargs)
            try:
                system.database.write(result)

            except DatabaseUnavailableException as e:
                logger.debug(f"Skipping persisting results: {str(e)}")
