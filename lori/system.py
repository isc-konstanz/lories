# -*- coding: utf-8 -*-
"""
lori.system
~~~~~~~~~~~


"""

from __future__ import annotations

import os
from functools import wraps
from shutil import copytree, ignore_patterns
from typing import List, Optional

from lori import ConfigurationException, Configurations, Settings
from lori.components import Component, WeatherProvider
from lori.components.context import ComponentContext
from lori.converters import ConverterAccess
from lori.connectors import ConnectorAccess
from lori.core import Activator, Context, Identifier, ResourceException
from lori.data import DataAccess, DataContext
from lori.location import Location, LocationUnavailableException
from lori.util import validate_key
from lori.weather import Weather, WeatherUnavailableException


# noinspection PyProtectedMember
class System(ComponentContext, Activator, Identifier):
    SECTION: str = "system"
    INCLUDES: List[str] = [ConverterAccess.SECTION, ConnectorAccess.SECTION, DataAccess.SECTION]

    __converters: ConverterAccess
    __connectors: ConnectorAccess
    __data: DataAccess

    _location: Optional[Location] = None

    # noinspection PyShadowingBuiltins
    @classmethod
    def copy(cls, settings: Settings) -> bool:
        configs = Configurations.load(f"{cls.__name__.lower()}.conf", **settings.dirs.to_dict())
        if "key" in configs:
            key = validate_key(configs["key"])
        elif "name" in configs:
            key = validate_key(configs["name"])
            configs["key"] = key
        else:
            raise ConfigurationException("Invalid configuration, missing specified system name")

        configs.dirs._data = os.path.join(configs.dirs.data, key)

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
                "logging*",
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

    # noinspection PyShadowingBuiltins, PyUnusedLocal
    def __init__(
        self,
        context: Context = None,
        configs: Optional[Configurations] = None,
        id: Optional[str] = None,
        key: Optional[str] = None,
        name: Optional[str] = None,
        **kwargs,
    ) -> None:
        key = self._build_key(key, configs=configs)
        super().__init__(
            context=context,
            configs=configs,
            id=self._build_id(id, key, configs=configs, context=context),
            name=self._build_name(name, configs=configs),
            key=key,
            **kwargs,
        )
        self.__converters = ConverterAccess(self)
        self.__connectors = ConnectorAccess(self)
        self.__data = DataAccess(self)

    # noinspection PyShadowingNames, PyArgumentList
    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        components = Context.__getattribute__(self, f"_{Context.__name__}__map")
        if attr in components.keys():
            return components[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no component '{attr}'")

    # noinspection PyShadowingBuiltins
    @classmethod
    def _assert_id(cls, id: Optional[str], key: Optional[str]) -> str:
        id = super()._assert_id(id, key)
        if len(id.split(".")) <= 1:
            raise ResourceException(f"Missing context in '{cls.__name__}' id: {id}")
        return id

    # noinspection PyShadowingBuiltins
    @classmethod
    def _build_id(
        cls,
        id: Optional[str],
        key: Optional[str],
        configs: Optional[Configurations],
        context: Context,
    ) -> str:
        if configs is not None:
            if "id" in configs:
                id = configs["id"]
        if id is None:
            id = f"{context.id}.{key}"
        return id

    @classmethod
    def _build_key(cls, key: str, configs: Optional[Configurations]) -> str:
        if configs is not None:
            if "key" in configs:
                key = configs["key"]
            elif "name" in configs:
                key = validate_key(configs["name"])
        return key

    @classmethod
    def _build_name(cls, name: Optional[str], configs: Optional[Configurations]) -> str:
        if configs is not None:
            if "name" in configs:
                name = configs["name"]
        return name

    # noinspection PyProtectedMember
    def _add(self, *components: Component) -> None:
        for component in components:
            super()._set(component.key, component)
            self.context.components._set(component.id, component)

    # noinspection PyProtectedMember
    def _update(self, context: Context, configs: Configurations) -> Component:
        component = self._new(context, configs)
        if component.key in self:
            self._get(component.key).configs.update(configs)
        else:
            self._add(component)
        return component

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.sort()
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

    @wraps(configure, updated=())
    def _do_configure(self, configs: Configurations, *args, **kwargs) -> None:
        if configs is None:
            raise ConfigurationException(f"Invalid NoneType configuration for {type(self).__name__}: {self.name}")
        if not configs.enabled:
            raise ConfigurationException(f"Trying to configure disabled {type(self).__name__}: {configs.name}")

        self.__converters.configure(configs.get_sections([ConverterAccess.SECTION], ensure_exists=True))
        self.__connectors.configure(configs.get_sections([ConnectorAccess.SECTION], ensure_exists=True))
        self.__data.configure(configs.get_section(DataAccess.SECTION, ensure_exists=True))
        super()._do_configure(configs, *args, **kwargs)
        self.__data.create()

    @property
    def converters(self):
        return self.__converters

    @property
    def connectors(self):
        return self.__connectors

    @property
    def data(self):
        return self.__data

    @property
    def location(self) -> Location:
        if self._location is None:
            raise LocationUnavailableException(f"System '{self.name}' has no location configured")
        return self._location

    # noinspection PyTypeChecker
    @property
    def weather(self) -> Weather:
        if not self.has_component(WeatherProvider):
            raise WeatherUnavailableException(f"System '{self.name}' has no weather configured")
        return next(self.get_component_type(WeatherProvider))
