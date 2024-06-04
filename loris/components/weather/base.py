# -*- coding: utf-8 -*-
"""
    loris.components.weather.base
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module provides the :class:`loris.components.weather.weather.Weather`, used as reference to
    calculate e.g. photovoltaic installations generated power. The provided environmental data
    contains temperatures and horizontal solar irradiation, which can be used, to calculate the
    effective irradiance on defined, tilted photovoltaic systems.

"""
from __future__ import annotations

from loris import Configurations, Location
from loris.components import Component, ComponentException, ComponentUnavailableException
from loris.components.weather import WeatherConnector
from loris.connectors import ConnectorContext


# noinspection SpellCheckingInspection
class WeatherBase(Component):
    TYPE = "weather"

    _connector: WeatherConnector = None

    @staticmethod
    def _load_connector(context: ConnectorContext, configs: Configurations, location: Location) -> WeatherConnector:
        connector_type = configs.get("type").lower()
        if connector_type in ["brightsky", "dwd"]:
            from .dwd import Brightsky

            return Brightsky(context, configs, location)

        elif connector_type in ["meteoblue", "nmm"]:
            from .meteoblue import Meteoblue

            return Meteoblue(context, configs, location)

    @property
    def connector(self) -> WeatherConnector:
        if not self._connector:
            raise WeatherException(f"Weather '{self.name}' has no connector available")
        return self._connector

    def get_type(self) -> str:
        return self.TYPE


class WeatherException(ComponentException):
    """
    Raise if an error occurred accessing the weather.

    """


class WeatherUnavailableException(ComponentUnavailableException, WeatherException):
    """
    Raise if a configured weather access can not be found.

    """
