# -*- coding: utf-8 -*-
"""
loris.components.weather.weather
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module provides the :class:`loris.components.weather.weather.Weather`, used as reference to
calculate e.g. photovoltaic installations generated power. The provided environmental data
contains temperatures and horizontal solar irradiation, which can be used, to calculate the
effective irradiance on defined, tilted photovoltaic systems.

"""

from __future__ import annotations

import datetime as dt
from typing import Optional

import pandas as pd
from loris import Configurations, Location
from loris.components import Component, ComponentException, ComponentUnavailableException
from loris.components.weather import WeatherConnector, WeatherForecast


# noinspection SpellCheckingInspection
class Weather(Component):
    TYPE = "weather"

    GHI = "ghi"
    DNI = "dni"
    DHI = "dhi"
    TEMP_AIR = "temp_air"
    TEMP_FELT = "temp_felt"
    TEMP_DEW_POINT = "dew_point"
    HUMIDITY_REL = "relative_humidity"
    PRESSURE_SEA = "pressure_sea"
    WIND_SPEED = "wind_speed"
    WIND_SPEED_GUST = "wind_speed_gust"
    WIND_DIRECTION = "wind_direction"
    WIND_DIRECTION_GUST = "wind_direction_gust"
    CLOUD_COVER = "cloud_cover"
    CLOUDS_LOW = "clouds_low"
    CLOUDS_MID = "clouds_mid"
    CLOUDS_HIGH = "clouds_high"
    SUNSHINE = "sunshine"
    VISIBILITY = "visibility"
    PRECIPITATION = "precipitation"
    PRECIPITATION_CONV = "precipitation_convective"
    PRECIPITATION_PROB = "precipitation_probability"
    PRECIPITABLE_WATER = "precipitable_water"
    SNOW_FRACTION = "snow_fraction"

    location: Location

    _forecast: Optional[WeatherForecast] = None
    _connector: Optional[WeatherConnector] = None

    # noinspection PyProtectedMember
    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        self.__localize__(configs)

        connector_type = configs.get("type", default="default").lower()
        connector_configs = configs.copy()
        connector_configs["id"] = connector_configs.get("id", default=connector_type).lower()
        connector_configs["uuid"] = f"{self._uuid}.{connector_configs['id']}"
        connector_configs.pop("data", None)
        connector_context = self._context.context.connectors

        if connector_type in ["default", "virtual"]:
            self._connector = None
        elif connector_type in ["brightsky", "dwd"]:
            from .dwd import Brightsky

            self._connector = Brightsky(connector_context, connector_configs, self.location)

        elif connector_type in ["meteoblue", "nmm"]:
            from .meteoblue import Meteoblue

            self._connector = Meteoblue(connector_context, connector_configs, self.location)

        if self._connector is not None:
            connector_context._add(self._connector)
            self._connector.configure()
            self._configs.update(self._connector._get_config_defaults(), replace=False)

            if self._connector.has_forecast() and self._configs.has_section(WeatherForecast.SECTION):
                self._configs[WeatherForecast.SECTION]["id"] = f"{self.id}.forecast"
                self._forecast = WeatherForecast(
                    self._context,
                    self._connector,
                    configs.get_section(WeatherForecast.SECTION)
                )
                self._forecast.configure()

        # TODO: configure default channels

    # noinspection PyMethodMayBeStatic
    def __localize__(self, configs: Configurations) -> None:
        if hasattr(self._context, "location"):
            location = getattr(self._context, "location")
            if not isinstance(location, Location):
                raise WeatherException(f"Invalid location type for weather '{self._uuid}': {type(location)}")
            self.location = location

        elif configs.has_section(Location.SECTION):
            location_configs = configs.get_section(Location.SECTION)
            self.location = Location(
                location_configs.get_float("latitude"),
                location_configs.get_float("longitude"),
                timezone=location_configs.get("timezone", default="UTC"),
                altitude=location_configs.get_float("altitude", default=None),
                country=location_configs.get("country", default=None),
                state=location_configs.get("state", default=None),
            )
        else:
            raise WeatherException(f"Unable to find valid location for weather configuration: {self.configs.name}")

    def __activate__(self) -> None:
        super().__activate__()
        if self.has_forecast():
            self._forecast.activate()

    def has_forecast(self) -> bool:
        return self._forecast is not None

    @property
    def forecast(self) -> WeatherForecast:
        if not self._forecast:
            raise WeatherException(f"Weather '{self.name}' has no forecast configured")
        return self._forecast

    @property
    def connector(self) -> WeatherConnector:
        if not self._connector:
            raise WeatherException(f"Weather '{self.name}' has no connector available")
        return self._connector

    def get_type(self) -> str:
        return self.TYPE

    def get(
        self,
        start: pd.Timestamp | dt.datetime = None,
        end: pd.Timestamp | dt.datetime = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Retrieves the weather data for a specified time interval

        :param start:
            the start timestamp for which weather data will be looked up for.
            For many applications, passing datetime.datetime.now() will suffice.
        :type start:
            :class:`pandas.Timestamp` or datetime

        :param end:
            the end timestamp for which weather data will be looked up for.
        :type end:
            :class:`pandas.Timestamp` or datetime

        :returns:
            the weather data, indexed in a specific time interval.

        :rtype:
            :class:`pandas.DataFrame`
        """
        return self._get_range(self.data.to_frame(), start, end)

    @staticmethod
    def _get_range(
        data: pd.DataFrame,
        start: pd.Timestamp | dt.datetime,
        end: pd.Timestamp | dt.datetime
    ) -> pd.DataFrame:
        if start is not None:
            data = data[data.index >= start]
        if end is not None:
            data = data[data.index <= end]
        return data


class WeatherException(ComponentException):
    """
    Raise if an error occurred accessing the weather.

    """


class WeatherUnavailableException(ComponentUnavailableException, WeatherException):
    """
    Raise if a configured weather access can not be found.

    """
