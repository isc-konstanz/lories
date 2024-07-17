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
from loris import ConfigurationException, Configurations, Location, LocationUnavailableException, Settings
from loris.components import Component, ComponentContext, Weather, WeatherUnavailableException
from loris.data.context import DataContext
from loris.util import parse_id


class System(ComponentContext, Component):
    TYPE: str = "system"

    _location: Optional[Location] = None

    # noinspection PyShadowingBuiltins
    @classmethod
    def copy(cls, settings: Settings) -> bool:
        configs = Configurations(f"{cls.__name__.lower()}.conf", **settings.dirs.encode())
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

    def __init__(self, context: DataContext, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(context, configs, *args, **kwargs)

    # noinspection PyShadowingNames
    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        components = ComponentContext.__getattribute__(self, f"_{ComponentContext.__name__}__components")
        if attr in components.keys():
            return components[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no component '{attr}'")

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.localize(configs.get_section(Location.SECTION, defaults={"enabled": False}))

    def localize(self, configs: Configurations) -> None:
        if configs.enabled:
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
    def uuid(self) -> str:
        return self._uuid

    @property
    def id(self) -> str:
        return self._uuid

    @property
    def name(self) -> str:
        return self._name

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
        return self.get_component_type(Weather.TYPE)[0]

    @property
    def type(self) -> str:
        return self.TYPE

    # noinspection PyMethodMayBeStatic
    def run(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        return None
