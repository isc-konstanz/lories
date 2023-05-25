# -*- coding: utf-8 -*-
"""
    corsys.cmpt.base
    ~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from typing import Optional, Dict, List
from collections.abc import Mapping

import os
import re
import pandas as pd

from ..cost import Cost, CostUnavailableException
from ..configs import Configurations, Configurable

# noinspection SpellCheckingInspection
INVALID_CHARS = "'!@#$%^&?*;:,./\\|`´+~=- "


class Component(Configurable):

    @classmethod
    def read(cls, system, conf_file: str = None) -> Component:
        return cls(system, Configurations.from_configs(system.configs, conf_file=conf_file))

    def __init__(self, system, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.system = system

    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)

        if not configs.has_option(Configurations.GENERAL, 'id'):
            raise ValueError('Invalid configuration, missing specified component id')

        self.id = configs.get(Configurations.GENERAL, 'id')
        self.name = configs.get(Configurations.GENERAL, 'name', fallback=configs.get(Configurations.GENERAL, 'id'))

        if configs.has_section(Cost.SECTION):
            self._cost = self.__cost__(configs)
        else:
            self._cost = None

    def __activate__(self, system) -> None:
        pass

    def __build__(self, **kwargs) -> Optional[pd.DataFrame]:
        pass

    def __cost__(self, configs: Configurations) -> Cost:
        return Cost(**dict(configs.items(Cost.SECTION)))

    @property
    def id(self) -> str:
        return self._id

    # noinspection PyAttributeOutsideInit
    @id.setter
    def id(self, s: str) -> None:
        self._id = re.sub('[^A-Za-z0-9_]+', '', s.translate({ord(c): "_" for c in INVALID_CHARS}))

    @property
    def name(self):
        return self._name

    # noinspection PyAttributeOutsideInit
    @name.setter
    def name(self, s: str) -> None:
        if s is None:
            self._name = s
        else:
            self._name = re.sub('[^A-Za-z0-9 ]+', '', s.translate({ord(c): " " for c in INVALID_CHARS+'_'}))

    @property
    def type(self) -> str:
        return 'component'

    @property
    def cost(self):
        if self._cost is None:
            raise CostUnavailableException(f"Component \"{self.name}\" has no costs configured")
        return self._cost

    def activate(self) -> None:
        self.__activate__(self.system)

    def build(self, **kwargs) -> Optional[pd.DataFrame]:
        return self.__build__(**kwargs)


class Components(Configurable, Mapping):

    @classmethod
    def _read(cls, **kwargs) -> Components:
        return cls(Configurations(f"{cls.__name__.lower()}.cfg", **kwargs))

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self._components = self.__readcmpts__()
        self.__activate__(self._components)

    def __activate__(self, components: Dict[str, Component]) -> None:
        pass

    def __build__(self, **kwargs) -> Optional[pd.DataFrame]:
        pass

    # noinspection PyUnresolvedReferences, SpellCheckingInspection
    def __readcmpts__(self) -> Dict[str, Component]:
        cmpt_dir = self.configs.dirs.cmpt

        components = dict()
        for entry in os.scandir(cmpt_dir):
            if entry.is_file() and entry.path.endswith('.cfg') \
                    and not entry.path.endswith('default.cfg') \
                    and not (entry.path.endswith('evaluation.cfg') or entry.path.endswith('evaluations.cfg')) \
                    and entry.name.startswith(tuple(self.__cmpt_types__())):
                # noinspection PyTypeChecker
                component = self.__readcmpt__(entry)
                if component.enabled:
                    components[component.id] = component

        return components

    # noinspection SpellCheckingInspection
    def __readcmpt__(self, conf_file: os.DirEntry) -> Component:
        component_configs = Configurations.from_configs(self.configs,
                                                        conf_file=conf_file.name,
                                                        conf_dir=os.path.dirname(conf_file.path))

        if Configurations.GENERAL not in component_configs.sections():
            component_configs.add_section(Configurations.GENERAL)
        if not component_configs.has_option(Configurations.GENERAL, 'id'):
            component_configs.set(Configurations.GENERAL, 'id', os.path.splitext(conf_file.name)[0])

        component_type = [t for t in self.__cmpt_types__() if conf_file.name.startswith(t)][0]
        component = self.__cmpt__(component_configs, component_type)

        if not isinstance(component, Component):
            raise TypeError(f"Invalid component type: {type(component)}")

        return component

    # noinspection PyShadowingBuiltins
    def __cmpt__(self, configs: Configurations, type: str) -> Component:
        if type == 'pv':
            from corsys.cmpt import Photovoltaic
            return Photovoltaic(self, configs)
        elif type == 'ev':
            from corsys.cmpt import ElectricVehicle
            return ElectricVehicle(self, configs)
        elif type == 'ees':
            from corsys.cmpt import ElectricalEnergyStorage
            return ElectricalEnergyStorage(self, configs)
        elif type == 'tes':
            from corsys.cmpt import ThermalEnergyStorage
            return ThermalEnergyStorage(self, configs)

        raise ValueError(f"Invalid component type: {type}")

    def __cmpt_types__(self, *args: str) -> List[str]:
        return ['component', 'cmpt', 'pv', 'ev', 'ees', 'tes', *args]

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        components = Components.__getattribute__(self, '_components')
        if attr in components.keys():
            return components[attr]
        try:
            # noinspection PyUnresolvedReferences
            return super().__getattr__(attr)

        except AttributeError:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(type(self).__name__, attr))

    def __getitem__(self, key: str) -> Component:
        return self._components[key]

    def __iter__(self):
        return iter(self._components)

    def __len__(self) -> int:
        return len(self._components)

    def get_types(self) -> List[str]:
        return self.__cmpt_types__()

    def get_type(self, *keys: str) -> List[Component] | Component:
        return [component for component in self._components.values()
                if any(key.startswith(component.type) for key in keys)]

    def contains_type(self, *keys):
        return len(self.get_type(*keys)) > 0

    def build(self, **kwargs) -> Optional[pd.DataFrame]:
        return self.__build__(**kwargs)
