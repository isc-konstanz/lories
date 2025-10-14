# -*- coding: utf-8 -*-
"""
lori.system
~~~~~~~~~~~


"""

from __future__ import annotations

import os
from typing import Any, List, Optional

import pandas as pd
from lori import Configurations
from lori.components import Component, ComponentContext, ComponentUnavailableError, Weather
from lori.core.typing import Timestamp
from lori.location import Location, LocationUnavailableException
from lori.simulation import Results


class System(Component):
    TYPE: str = "system"

    _location: Optional[Location] = None

    @classmethod
    def scan(cls, context: ComponentContext, scan_dir: str, **kwargs) -> List[System]:
        systems = []
        for system_dir in os.scandir(scan_dir):
            if os.path.isdir(system_dir.path):
                kwargs["data_dir"] = system_dir.path
                systems.append(cls.load(context, **kwargs))
        return systems

    # noinspection PyProtectedMember
    @classmethod
    def load(cls, context: ComponentContext, **kwargs) -> System:
        configs = Configurations.load(f"{cls.__name__.lower()}.conf", **kwargs)
        system_id = cls._build_id(context=context, configs=configs)
        if context._contains(system_id):
            context._update(system_id, configs)
            system = context._get(system_id)
        else:
            system = cls(context=context, configs=configs, id=system_id)
            context._add(system)
        return system

    # noinspection PyShadowingBuiltins, PyUnusedLocal
    def __init__(
        self,
        context: ComponentContext = None,
        configs: Optional[Configurations] = None,
        id: Optional[str] = None,
        key: Optional[str] = None,
        name: Optional[str] = None,
        **kwargs,
    ) -> None:
        name = self._build_name(name, configs=configs)
        key = self._build_key(key, configs=configs)
        id = self._build_id(id, key, configs=configs, context=context)
        super().__init__(
            context=context,
            configs=configs,
            id=id,
            key=key,
            name=name,
            **kwargs,
        )

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.localize(configs.get_member(Location.TYPE, defaults={}))
        self.components.load(configs_dir=self.configs.dirs.conf)

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

    def is_localized(self) -> bool:
        return self._location is not None

    @property
    def location(self) -> Location:
        if self._location is None:
            raise LocationUnavailableException(f"System '{self.name}' has no location configured")
        return self._location

    def has_weather(self) -> bool:
        return self.components.has_type(Weather)

    # noinspection PyTypeChecker
    @property
    def weather(self) -> Weather:
        weather = self.components.get_first(Weather)
        if weather is None:
            raise ComponentUnavailableError(f"System '{self.name}' has no weather configured")
        return weather

    def simulate(
        self,
        start: Timestamp,
        end: Timestamp,
        prior: Optional[pd.DataFrame] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        raise NotImplementedError()

    def evaluate(self, results: Results) -> pd.DataFrame:
        raise NotImplementedError()
