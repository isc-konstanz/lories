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
from typing import Collection, Optional

import pandas as pd
from loris import Configurations, Configurator, Location, LocationUnavailableException
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

    _location: Location

    _forecast: Optional[WeatherForecast] = None
    _connector: Optional[WeatherConnector] = None

    # noinspection PyProtectedMember
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.localize(configs.get_section(Location.SECTION, defaults={"enabled": False}))

        connector_type = configs.get("type", default="virtual").lower()
        connector_context = self.context.context.connectors
        connector_configs = configs.get_section(connector_type, defaults={})
        connector_configs["id"] = configs.get("id", default=connector_type).lower()
        connector_configs["uuid"] = f"{self.uuid}.{configs['id']}"
        if connector_type != "virtual":
            self._connector = WeatherConnector.load(
                connector_type,
                connector_context,
                connector_configs,
                self._location
            )
            if self._connector.has_forecast() and configs.has_section(WeatherForecast.SECTION):
                self.configs[WeatherForecast.SECTION]["id"] = f"{self.id}_forecast"
                self._forecast = WeatherForecast(
                    self,
                    self._connector,
                    configs.get_section(WeatherForecast.SECTION)
                )
            connector_context._add(self._connector)

        # TODO: configure default channels

    # noinspection PyProtectedMember
    def _do_configure_members(self, configurators: Collection[Configurator]) -> None:
        configurators = list(configurators)

        # Update local configurator variables manually to ensure correct order
        if self._has_connector():
            self._connector._do_configure()
            self.configs.update(self._connector._get_config_defaults(), replace=False)
            configurators.remove(self._connector)
        if self.has_forecast():
            self._forecast._do_configure()
            configurators.remove(self._forecast)

        super()._do_configure_members(configurators)

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
            try:
                self._location = self.context.location
                if not isinstance(self._location, Location):
                    raise WeatherException(f"Invalid location type for weather '{self.uuid}': {type(self._location)}")
            except (LocationUnavailableException, AttributeError) as e:
                raise WeatherException(f"Unable to find valid location for weather: {self.name}", e)

    def activate(self) -> None:
        super().activate()

    def deactivate(self) -> None:
        super().deactivate()

    def has_forecast(self) -> bool:
        return self._forecast is not None

    @property
    def forecast(self) -> WeatherForecast:
        if not self._forecast:
            raise WeatherException(f"Weather '{self.name}' has no forecast configured")
        return self._forecast

    def _has_connector(self) -> bool:
        return self._connector is not None

    @property
    def connector(self) -> WeatherConnector:
        if not self._connector:
            raise WeatherException(f"Weather '{self.name}' has no connector available")
        return self._connector

    @property
    def type(self) -> str:
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
