# -*- coding: utf-8 -*-
"""
loris.system
~~~~~~~~~~~~


"""

from __future__ import annotations

import os
from shutil import copytree, ignore_patterns
from typing import List, Optional

import pandas as pd
from loris import ConfigurationException, Configurations, Settings
from loris.components import Component, Weather, WeatherUnavailableException
from loris.components.context import ComponentContext
from loris.core import Context, RegistratorContext
from loris.data.context import DataContext
from loris.location import Location, LocationUnavailableException
from loris.util import parse_id


class System(Component, ComponentContext):
    TYPE: str = "system"

    _location: Optional[Location] = None

    # noinspection PyShadowingBuiltins
    @classmethod
    def copy(cls, settings: Settings) -> bool:
        configs = Configurations.load(f"{cls.__name__.lower()}.conf", **settings.dirs.encode())
        if "id" in configs:
            id = parse_id(configs["id"])
        elif "name" in configs:
            id = parse_id(configs["name"])
            configs["id"] = id
        else:
            raise ConfigurationException("Invalid configuration, missing specified system name")

        configs.dirs._data = os.path.join(configs.dirs.data, id)

        if os.path.isdir(configs.dirs.data):
            return False
        os.makedirs(configs.dirs.data, exist_ok=True)

        copytree(
            settings.dirs.conf,
            configs.dirs.conf,
            ignore=ignore_patterns(
                "*.default.conf",
                "evaluations*",
                "results*",
                "settings*",
                "logging*"
            ),
        )
        return True

    # noinspection PyUnresolvedReferences
    @classmethod
    def scan(cls, context: DataContext, scan_dir: str, **kwargs) -> List[System]:
        systems = []
        for system_dir in os.scandir(scan_dir):
            if os.path.isdir(system_dir.path):
                kwargs["data_dir"] = system_dir.path
                systems.append(cls.load(context, **kwargs))
        return systems

    @classmethod
    def load(cls, context: DataContext = None, **kwargs) -> System:
        return cls(context, Configurations.load(f"{cls.__name__.lower()}.conf", **kwargs))

    def __repr__(self) -> str:
        return Component.__repr__(self)

    def __str__(self) -> str:
        return Component.__str__(self)

    # noinspection PyShadowingNames, PyArgumentList
    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        components = RegistratorContext.__getattribute__(self, f"_{RegistratorContext.__name__}__map")
        if attr in components.keys():
            return components[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no component '{attr}'")

    # noinspection PyProtectedMember
    def _add(self, *components: Component) -> None:
        for component in components:
            super()._set(component.id, component)
            self.context.components._set(component.uuid, component)

    # noinspection PyProtectedMember
    def _update(self, context: Context, configs: Configurations) -> Component:
        component = self._new(context, configs)
        if component.id in self:
            self._get(component.id).configs.update(configs)
        else:
            self._add(component)
        return component

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._sort()
        self.localize(configs.get_section(Location.SECTION, defaults={}))

    def localize(self, configs: Configurations) -> None:
        if configs.enabled and all(k in configs for k in ["latitude", "longitude"]):
            self._location = Location(
                configs.get_float("latitude"),
                configs.get_float("longitude"),
                timezone=configs.get("timezone", default="UTC"),
                altitude=configs.get_float("altitude", default=None),
                country=configs.get("country", default=None),
                state=configs.get("state", default=None),
            )
        else:
            self._location = None

    @property
    def location(self) -> Location:
        if self._location is None:
            raise LocationUnavailableException(f"System '{self.name}' has no location configured")
        return self._location

    # noinspection PyTypeChecker
    @property
    def weather(self) -> Weather:
        if not self.has_component(Weather.TYPE):
            raise WeatherUnavailableException(f"System '{self.name}' has no weather configured")
        return next(self.get_component_type(Weather.TYPE))

    @property
    def type(self) -> str:
        return self.TYPE

    # noinspection PyMethodMayBeStatic
    def run(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        return None
