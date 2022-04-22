# -*- coding: utf-8 -*-
"""
    th-e-core.system
    ~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from collections.abc import MutableSequence, MutableMapping
from typing import Dict, List, Tuple, Iterator

import os
import re
import logging
import pandas as pd

from configparser import ConfigParser as Configurations
from th_e_core import Configurable, Database

logger = logging.getLogger(__name__)

INVALID_CHARS = "'!@#$%^&?*;:,./\|`´+~=- "


class Systems(MutableSequence):

    def __init__(self, *systems: System) -> None:
        self._systems = list()
        self._systems.extend(systems)

    def __iter__(self) -> Iterator[System]:
        return iter(self._systems)

    def __len__(self) -> int:
        return len(self._systems)

    def __getitem__(self, index: int) -> System:
        return self._systems[index]

    def __delitem__(self, index: int) -> None:
        del self._systems[index]

    def __setitem__(self, index: int, system: System) -> None:
        self._systems[index] = system

    def insert(self, index: int, system) -> None:
        self._systems.insert(index, system)

    def run(self, *args, **kwargs) -> None:
        for system in self:
            system.run(*args, **kwargs)


class System(Configurable, MutableMapping):

    POWER_EL = 'el_power'
    POWER_EL_IMP = 'el_imp_power'
    POWER_EL_EXP = 'el_exp_power'
    POWER_TH = 'th_power'
    POWER_TH_HT = 'th_ht_power'
    POWER_TH_DOM = 'th_dom_power'

    ENERGY_EL = 'el_energy'
    ENERGY_EL_IMP = 'el_import_energy'
    ENERGY_EL_EXP = 'el_export_energy'
    ENERGY_TH = 'th_energy'
    ENERGY_TH_HT = 'th_ht_energy'
    ENERGY_TH_DOM = 'th_dom_energy'

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
                    systems.append(cls._read(data_dir=system_dir.path, **kwargs))
        else:
            systems.append(cls._read(data_dir=data_dir, **kwargs))

        return systems

    @classmethod
    def _read(cls,
              root_dir:    str = '.',
              lib_dir:     str = 'lib',
              tmp_dir:     str = 'tmp',
              data_dir:    str = 'data',
              cmpt_dir:    str = 'cmpt',
              config_dir:  str = 'conf',
              config_name: str = 'system.cfg',
              **kwargs) -> System:

        configs = cls._read_configs(root_dir, lib_dir, tmp_dir, data_dir, config_dir, config_name, **kwargs)
        config_dir = configs.get('General', 'config_dir')
        cmpt_dir = configs.get('General', 'cmpt_dir', fallback=cmpt_dir)
        if "~" in cmpt_dir:
            cmpt_dir = os.path.expanduser(cmpt_dir)
        if not os.path.isabs(cmpt_dir):
            cmpt_dir = os.path.join(config_dir, cmpt_dir)
        if cmpt_dir == os.path.join(config_dir, 'ems') and not os.path.isdir(cmpt_dir):
            cmpt_dir = config_dir

        configs.set('General', 'cmpt_dir', cmpt_dir)

        return cls._from_class(configs)

    def __init__(self, configs: Configurations, **kwargs) -> None:
        if not configs.has_option('General', 'name'):
            raise ValueError('Invalid configuration, missing specified system name')

        if configs.has_option('General', 'id'):
            self.id = configs['General']['id']
        else:
            self.id = configs['General']['name']
            configs['General']['id'] = self.id

        self.name = configs['General']['name']

        super().__init__(configs, **kwargs)
        self._components = self._components_read()
        self._activate(self._components, configs)

    def _configure(self, configs: Configurations) -> None:
        super()._configure(configs)

        if configs.has_section('Location'):
            from pvlib.location import Location
            self.location = Location(configs.getfloat('Location', 'latitude'),
                                     configs.getfloat('Location', 'longitude'),
                                     tz=configs.get('Location', 'timezone', fallback='UTC'),
                                     altitude=configs.getfloat('Location', 'altitude', fallback=0),
                                     name=self.name)

    def _activate(self, components: Dict[str, Component], configs: Configurations) -> None:
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
                    configs.set('Database', 'timezone', self.location.tz)

            self._database = Database.open(configs)
        else:
            self._database = None

    def _components_read(self) -> Dict[str, Component]:
        cmpt_dir = self._configs.get('General', 'cmpt_dir', fallback='cmpt')

        components = dict()
        for entry in os.scandir(cmpt_dir):
            if entry.is_file() and entry.path.endswith('.cfg') and not entry.path.endswith('default.cfg') \
                    and entry.name.startswith(tuple(self._component_types)):
                component = self._component_read(entry)
                components[component.id] = component

        return components

    def _component_read(self, config_file: os.DirEntry) -> Component:
        component_configs = Configurable._read_configs(self.configs.get('General', 'root_dir'),
                                                       self.configs.get('General', 'lib_dir'),
                                                       self.configs.get('General', 'tmp_dir'),
                                                       self.configs.get('General', 'data_dir'),
                                                       os.path.dirname(config_file.path),
                                                       config_file.name)

        if not component_configs.has_option('General', 'id'):
            component_configs.set('General', 'id', os.path.splitext(config_file.name)[0])

        component_type = [t for t in self._component_types if config_file.name.startswith(t)][0]
        component = self._component(component_configs, component_type)

        if not isinstance(component, Component):
            raise TypeError('Invalid component type: {}'.format(type(component)))

        return component

    # noinspection PyShadowingBuiltins
    def _component(self, configs: Configurations, type: str) -> Component:
        if type == 'pv':
            # noinspection PyUnresolvedReferences
            from th_e_core.pv import PVSystem
            return PVSystem(self, configs)

        return ConfigComponent(self, configs)

    @property
    def _component_types(self) -> List[str]:
        return ['component', 'cmpt', 'pv']

    def contains_type(self, key):
        return len(self.get_type(key)) > 0

    def get_type(self, key: str) -> List[Component]:
        return [component for component in self._components.values() if component.type in key]

    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, s: str) -> None:
        for c in INVALID_CHARS:
            s = s.replace(c, '_')

        self._id = re.sub('[^A-Za-z0-9_]+', '', s).lower()

    def __getattr__(self, attr):
        if attr in self._component_types:
            return self._components[self.__keytransform__(attr)]
        try:
            return super().__getattr__(attr)

        except AttributeError:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(type(self).__name__, attr))

    def __getitem__(self, key: str) -> Component:
        return self._components[self.__keytransform__(key)]

    def __setitem__(self, key: str, value: Component) -> None:
        self._components[self.__keytransform__(key)] = value

    def __delitem__(self, key: str) -> None:
        del self._components[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self._components)

    def __len__(self) -> int:
        return len(self._components)

    @staticmethod
    def __keytransform__(key):
        return key


    def run(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError


class Component(Configurable):

    def __init__(self, system: System, configs: Configurations, **kwargs) -> None:
        super().__init__(configs, **kwargs)

        if not configs.has_option('General', 'id'):
            raise ValueError('Invalid configuration, missing specified component id')

        self.id = configs.get('General', 'id')
        self._system = system

        self._activate(system, **kwargs)

    def _activate(self, system: System, **kwargs):
        pass

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, s):
        self._id = re.sub('[^A-Za-z0-9]+', '', s.translate({ord(c): "_" for c in INVALID_CHARS}))

    @property
    def type(self):
        return 'cmpt'


class ConfigComponent(Component, MutableMapping):

    def __setitem__(self, key: str, value: str) -> None:
        self._configs.set('General', key, value)

    def __getitem__(self, key: str) -> str:
        return self._configs.get('General', key)

    def __delitem__(self, key: str) -> None:
        self._configs.remove_option('General', key)

    def __iter__(self) -> Iterator[Tuple[str, str]]:
        return iter(self._configs.items('General'))

    def __len__(self) -> int:
        return len(self._configs.items('General'))

    @property
    def type(self):
        return 'cfg'
