# -*- coding: utf-8 -*-
"""
lori.weather
~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from typing import Optional

import pandas as pd
from lori.core import Activator, Configurations, Registrator, ResourceException, ResourceUnavailableException
from lori.location import Location, LocationUnavailableException


# noinspection SpellCheckingInspection
class Weather(Registrator, Activator):
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

    # noinspection PyProtectedMember
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.localize(configs.get_section(Location.SECTION, defaults={}))

    def localize(self, configs: Configurations) -> None:
        if configs.enabled and all(k in configs for k in ["latitude", "longitude"]):
            self.location = Location(
                configs.get_float("latitude"),
                configs.get_float("longitude"),
                timezone=configs.get("timezone", default="UTC"),
                altitude=configs.get_float("altitude", default=None),
                country=configs.get("country", default=None),
                state=configs.get("state", default=None),
            )
        else:
            try:
                self.location = self.context.location
                if not isinstance(self.location, Location):
                    raise WeatherException(f"Invalid location type for weather '{self.id}': {type(self.location)}")
            except (LocationUnavailableException, AttributeError):
                raise WeatherException(f"Missing location for weather '{self.id}'")

    @abstractmethod
    def get(
        self,
        start: Optional[pd.Timestamp, dt.datetime, str] = None,
        end: Optional[pd.Timestamp, dt.datetime, str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Retrieves the weather data for a specified time interval

        :param start:
            the start timestamp for which weather data will be looked up for.
            For many applications, passing datetime.datetime.now() will suffice.
        :type start:
            :class:`pandas.Timestamp`, datetime or str

        :param end:
            the end timestamp for which weather data will be looked up for.
        :type end:
            :class:`pandas.Timestamp`, datetime or str

        :returns:
            the weather data, indexed in a specific time interval.

        :rtype:
            :class:`pandas.DataFrame`
        """
        pass


class WeatherException(ResourceException):
    """
    Raise if an error occurred accessing the weather.

    """


class WeatherUnavailableException(ResourceUnavailableException, WeatherException):
    """
    Raise if a configured weather access can not be found.

    """
