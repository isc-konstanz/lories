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

from .io import Database, DatabaseUnavailableException
from .configs import Configurations
from .component import Component, Context
from .location import Location

# noinspection SpellCheckingInspection
INVALID_CHARS = "'!@#$%^&?*;:,./\\|`´+~=- "

logger = logging.getLogger(__name__)


class System(Context):

    POWER_EL = 'el_power'
    POWER_EL_IMP = 'el_import_power'
    POWER_EL_EXP = 'el_import_power'
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
    def read(cls, data_dir: str = 'data', config_scan: bool = False, **kwargs) -> Systems:
        systems = Systems()

        if not isinstance(config_scan, bool):
            config_scan = str(config_scan).lower() == 'true'
        kwargs['config_scan'] = config_scan
        kwargs['config_copy'] = True

        if config_scan:
            for system_dir in os.scandir(data_dir):
                if os.path.isdir(system_dir.path):
                    systems._systems.append(cls._read(data_dir=system_dir.path, **kwargs))
        else:
            systems._systems.append(cls._read(data_dir=data_dir, **kwargs))

        return systems

    def __init__(self, configs: Configurations, **kwargs) -> None:
        super().__init__(configs, **kwargs)
        self.__activate__(self._components, configs)

    # noinspection PyMethodMayBeStatic
    def __init_location__(self, configs: Configurations) -> Location:
        return Location(configs.getfloat('Location', 'latitude'),
                        configs.getfloat('Location', 'longitude'),
                        timezone=configs.get('Location', 'timezone', fallback='UTC'),
                        altitude=configs.getfloat('Location', 'altitude', fallback=None),
                        country=configs.get('Location', 'country', fallback=None),
                        state=configs.get('Location', 'state', fallback=None))

    # noinspection PyUnresolvedReferences
    def __activate__(self, components: Dict[str, Component], configs: Configurations) -> None:
        super().__activate__(components, configs)
        if configs.has_section('Location'):
            self._location = self.__init_location__(configs)
        else:
            self._location = None

        if configs.has_section('Database') and \
                configs.get('Database', 'enabled', fallback='True').lower() == 'true' and \
                configs.get('Database', 'enable', fallback='True').lower() == 'true':
            if configs.get('Database', 'type').lower() == 'csv':
                database_dir = configs.get('Database', 'dir')
                database_central = configs.getboolean('Database', 'central', fallback=False)
                if database_central:
                    data_dir = configs['General']['lib_dir']
                else:
                    data_dir = configs['General']['data_dir']

                if not os.path.isabs(database_dir):
                    database_dir = os.path.join(data_dir, database_dir)
                if database_central:
                    database_dir = os.path.join(
                        database_dir,
                        '{0:08.4f}'.format(float(self.location.latitude)).replace('.', '') + '_' +
                        '{0:08.4f}'.format(float(self.location.longitude)).replace('.', '')
                    )
                configs.set('Database', 'dir', database_dir)

            if not configs.has_option('Database', 'timezone'):
                configs.set('Database', 'timezone', self.location.timezone.zone)

            self._database = Database.open(configs)
        else:
            self._database = None

    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, s: str) -> None:
        for c in INVALID_CHARS:
            s = s.replace(c, '_')

        self._id = re.sub('[^A-Za-z0-9_]+', '', s).lower()

    @property
    def name(self):
        return self._name

    @property
    def database(self):
        if self._database is None:
            raise DatabaseUnavailableException("System '{}' has no database configured".format(self.name))

        return self._database

    @property
    def location(self) -> Location:
        if not self._location:
            raise AttributeError("System '{}' has no location configured".format(self.name))

        return self._location

    def build(self, **kwargs) -> None:
        from th_e_data import build
        build(self._configs,
              self._database, **kwargs)

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
            system(*args, **kwargs)
