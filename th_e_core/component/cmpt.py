# -*- coding: utf-8 -*-
"""
    th-e-core.component.component
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from collections.abc import Mapping
from typing import Dict, List
import os
import re
from ..configs import Configurations, Configurable

# noinspection SpellCheckingInspection
INVALID_CHARS = "'!@#$%^&?*;:,./\\|`´+~=- "


class Component(Configurable):

    def __init__(self, context: Context, configs: Configurations, **kwargs) -> None:
        super().__init__(configs, **kwargs)

        if not configs.has_option('General', 'id'):
            raise ValueError('Invalid configuration, missing specified component id')

        self.id = configs.get('General', 'id')
        self.name = configs.get('General', 'name', fallback=configs.get('General', 'id'))
        self._context = context

        self.__activate__(context, **kwargs)

    def __activate__(self, context: Context, **kwargs):
        pass

    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, s: str) -> None:
        self._id = re.sub('[^A-Za-z0-9_]+', '', s.translate({ord(c): "_" for c in INVALID_CHARS}))

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, s: str) -> None:
        if s is None:
            self._name = s
        else:
            self._name = re.sub('[^A-Za-z0-9 ]+', '', s.translate({ord(c): " " for c in INVALID_CHARS+'_'}))

    @property
    def type(self) -> str:
        return 'component'


class Context(Configurable, Mapping):

    @classmethod
    def _read(cls,
              root_dir:    str = '.',
              lib_dir:     str = 'lib',
              tmp_dir:     str = 'tmp',
              data_dir:    str = 'data',
              cmpt_dir:    str = 'component',
              config_dir:  str = 'conf',
              config_file: str = 'system.cfg',
              **kwargs) -> Context:

        configs = cls._read_configs(root_dir, lib_dir, tmp_dir, data_dir, config_dir, config_file, **kwargs)
        config_dir = configs.get('General', 'config_dir')
        cmpt_dir = configs.get('General', 'cmpt_dir', fallback=cmpt_dir)
        if "~" in cmpt_dir:
            cmpt_dir = os.path.expanduser(cmpt_dir)
        if not os.path.isabs(cmpt_dir):
            cmpt_dir = os.path.join(config_dir, cmpt_dir)
        if cmpt_dir == os.path.join(config_dir, 'component') and not os.path.isdir(cmpt_dir):
            cmpt_dir = config_dir

        configs.set('General', 'cmpt_dir', cmpt_dir)

        return cls._from_class(configs)

    def __init__(self, configs: Configurations, **kwargs) -> None:
        if not configs.has_option('General', 'name'):
            raise ValueError("Invalid configuration, missing specified system name")

        if configs.has_option('General', 'id'):
            self.id = configs['General']['id']
        else:
            self.id = configs['General']['name']
            configs['General']['id'] = self.id

        self._name = configs['General']['name']

        super().__init__(configs, **kwargs)
        self._components = self.__readcmpts__()

    # noinspection SpellCheckingInspection
    def __readcmpts__(self) -> Dict[str, Component]:
        cmpt_dir = self._configs.get('General', 'cmpt_dir', fallback='component')

        components = dict()
        for entry in os.scandir(cmpt_dir):
            if entry.is_file() and entry.path.endswith('.cfg') \
                    and not entry.path.endswith('default.cfg') \
                    and not (entry.path.endswith('evaluation.cfg') or entry.path.endswith('evaluations.cfg')) \
                    and entry.name.startswith(tuple(self.__cmpt_types__())):
                # noinspection PyTypeChecker
                component = self.__readcmpt__(entry)
                components[component.id] = component

        return components

    # noinspection SpellCheckingInspection
    def __readcmpt__(self, config_file: os.DirEntry) -> Component:
        component_configs = Configurable._read_configs(self.configs.get('General', 'root_dir'),
                                                       self.configs.get('General', 'lib_dir'),
                                                       self.configs.get('General', 'tmp_dir'),
                                                       self.configs.get('General', 'data_dir'),
                                                       os.path.dirname(config_file.path),
                                                       config_file.name)

        if not component_configs.has_option('General', 'id'):
            component_configs.set('General', 'id', os.path.splitext(config_file.name)[0])

        component_type = [t for t in self.__cmpt_types__() if config_file.name.startswith(t)][0]
        component = self.__cmpt__(component_configs, component_type)

        if not isinstance(component, Component):
            raise TypeError(f"Invalid component type: {type(component)}")

        return component

    # noinspection SpellCheckingInspection, PyShadowingBuiltins
    def __cmpt__(self, configs: Configurations, type: str) -> Component:
        if type == 'pv':
            from th_e_core.component import Photovoltaics
            return Photovoltaics(self, configs)
        elif type == 'ev':
            from th_e_core.component import ElectricVehicle
            return ElectricVehicle(self, configs)
        elif type == 'ees':
            from th_e_core.component import ElectricalEnergyStorage
            return ElectricalEnergyStorage(self, configs)
        elif type == 'tes':
            from th_e_core.component import ThermalEnergyStorage
            return ThermalEnergyStorage(self, configs)

        raise ValueError(f"Invalid component type: {type}")

    def __cmpt_types__(self, *args: str) -> List[str]:
        return ['component', 'component', 'pv', 'ev', 'ees', 'tes', *args]

    # def __getattr__(self, attr):
    #     if attr in self._components.keys():
    #         return self._components[attr]
    #     try:
    #         # noinspection PyUnresolvedReferences
    #         return super().__getattr__(attr)
    #
    #     except AttributeError:
    #         raise AttributeError("'{0}' object has no attribute '{1}'".format(type(self).__name__, attr))

    def __getitem__(self, key: str) -> Component:
        return self._components[key]

    def __iter__(self):
        return iter(self._components)

    def __len__(self) -> int:
        return len(self._components)

    def get_types(self) -> List[str]:
        return self.__cmpt_types__()

    def get_type(self, key: str) -> List[Component]:
        return [component for component in self._components.values() if component.type in key]

    def contains_type(self, key):
        return len(self.get_type(key)) > 0

