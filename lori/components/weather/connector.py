# -*- coding: utf-8 -*-
"""
lori.components.weather.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori import ResourceException
from lori.components import Component
from lori.connectors import Connector
from lori.core import Configurations
from lori.location import Location, LocationUnavailableException
from lori.weather import WeatherException


# noinspection PyAbstractClass
class WeatherConnector(Connector):
    location: Location

    def __init__(self, component: Component, **kwargs) -> None:
        super().__init__(component, component.configs, **kwargs)
        self.__component = component

    @classmethod
    def _assert_context(cls, context: Component):
        from lori.components.weather import WeatherProvider

        if context is None or not isinstance(context, WeatherProvider):
            raise ResourceException(f"Invalid '{cls.__name__}' context: {type(context)}")
        return super()._assert_context(context)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.localize()

    # noinspection PyUnresolvedReferences
    def localize(self) -> None:
        try:
            self.location = self.__component.location
            if not isinstance(self.location, Location):
                raise WeatherException(
                    f"Invalid location type for weather connector '{self.id}': {type(self.location)}"
                )
        except (LocationUnavailableException, AttributeError):
            raise WeatherException(f"Missing location for weather connector '{self.id}'")
