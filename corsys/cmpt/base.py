# -*- coding: utf-8 -*-
"""
    corsys.cmpt.base
    ~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from collections.abc import Mapping
from typing import Dict, List
import os
import re
from ..cost import Cost, CostUnavailableException
from ..configs import Configurations, Configurable

# noinspection SpellCheckingInspection
INVALID_CHARS = "'!@#$%^&?*;:,./\\|`´+~=- "


class Component(Configurable):

    @classmethod
    def read(cls, context: Context, conf_file: str = None) -> Component:
        return cls(context, Configurations.from_configs(context.configs, conf_file=conf_file))

    def __init__(self, context: Context, configs: Configurations, **kwargs) -> None:
        super().__init__(configs, **kwargs)
        self._context = context
        self.__activate__(context, **kwargs)

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

    def __activate__(self, context: Context, **kwargs):
        pass

    # noinspection PyMethodMayBeStatic
    def __cost__(self, configs: Configurations) -> Cost:
        return Cost(dict(configs.items(Cost.SECTION)))

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

    @property
    def context(self) -> Context:
        return self._context


class Context(Configurable, Mapping):

    @classmethod
    def _read(cls, **kwargs) -> Context:
        return cls(Configurations(f"{cls.__name__.lower()}.cfg", **kwargs))

    def __init__(self, configs: Configurations, **kwargs) -> None:
        super().__init__(configs, **kwargs)
        self._components = self.__readcmpts__()

    # noinspection SpellCheckingInspection
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

    # noinspection SpellCheckingInspection, PyShadowingBuiltins
    def __cmpt__(self, configs: Configurations, type: str) -> Component:
        if type == 'pv':
            from corsys.cmpt import Photovoltaics
            return Photovoltaics(self, configs)
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
        if attr in self._components.keys():
            return self._components[attr]
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

    def get_type(self, key: str) -> List[Component]:
        return [component for component in self._components.values() if component.type in key]

    def contains_type(self, key):
        return len(self.get_type(key)) > 0
