# -*- coding: utf-8 -*-
"""
loris.system
~~~~~~~~~~~~


"""

from __future__ import annotations

import os
from typing import List, Optional

import pandas as pd
from loris import ConfigurationException, Configurations, Location, LocationUnavailableException, components
from loris.components import Component, ComponentContext, Weather, WeatherUnavailableException
from loris.data import DataAccess
from loris.data.context import DataContext
from loris.util import parse_id

components.register(Weather, Weather.TYPE)


class System(Component, ComponentContext):
    TYPE: str = "system"

    _location: Location = None

    @classmethod
    def load(cls, context: DataContext, **kwargs) -> System:
        return cls(context, Configurations.load(f"{cls.__name__.lower()}.conf", **kwargs))

    # noinspection PyUnresolvedReferences
    @classmethod
    def scan(cls, context: DataContext, scan_dir: str, **kwargs) -> List[System]:
        systems = []
        for system_dir in os.scandir(scan_dir):
            if os.path.isdir(system_dir.path):
                kwargs["data_dir"] = system_dir.path
                systems.append(cls.load(context, **kwargs))
        return systems

    def __init__(self, context: DataContext, configs: Configurations, *args, **kwargs) -> None:
        if "id" in configs:
            self._id = self._uuid = parse_id(configs.get("id"))
            self._name = configs.get("name", default=configs.get("id"))
        elif "name" in configs:
            self._id = self._uuid = parse_id(configs["id"] if "id" in configs else configs["name"])
            self._name = configs["name"]
        else:
            raise ConfigurationException("Invalid configuration, missing specified system ID")

        self.data = DataAccess(self, context, configs.get_section("data", default={}))
        super(Component, self).__init__(context, configs, *args, **kwargs)

    # noinspection PyShadowingNames
    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        components = ComponentContext.__getattribute__(self, "_components")
        if attr in components.keys():
            return components[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no component '{attr}'")

    # noinspection PyProtectedMember
    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        self.__localize__(configs)
        self._context.connectors._load_file(configs.dirs.conf, "connectors.conf", self.uuid)

    # noinspection PyMethodMayBeStatic
    def __localize__(self, configs: Configurations) -> None:
        if configs.has_section(Location.SECTION):
            location_configs = configs.get_section(Location.SECTION)
            self._location = Location(
                location_configs.get_float("latitude"),
                location_configs.get_float("longitude"),
                timezone=location_configs.get("timezone", default="UTC"),
                altitude=location_configs.get_float("altitude", default=None),
                country=location_configs.get("country", default=None),
                state=location_configs.get("state", default=None),
            )

    # noinspection PyUnresolvedReferences
    def activate(self) -> None:
        self._logger.info(f"Activating {type(self).__name__}: {self.name}")
        super(Component, self).activate()
        self._active = True

    # noinspection PyUnresolvedReferences
    def deactivate(self) -> None:
        self._logger.info(f"Deactivating {type(self).__name__}: {self.name}")
        super(Component, self).deactivate()
        self._active = False

    @property
    def location(self) -> Location:
        if not self._location:
            raise LocationUnavailableException(f"System '{self.name}' has no location configured")
        return self._location

    # noinspection PyTypeChecker
    @property
    def weather(self) -> Weather:
        if not self.has_component(Weather.TYPE):
            raise WeatherUnavailableException(f"System '{self.name}' has no weather configured")
        return self.get_component_type(Weather.TYPE)[0]

    def get_type(self):
        return self.TYPE

    # noinspection PyMethodMayBeStatic
    def run(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        return None
