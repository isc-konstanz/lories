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

from loris import Configurations, Context, Location, LocationUnavailableException
from loris.components import Component, ComponentException, ComponentUnavailableException


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

    # noinspection PyShadowingBuiltins
    @classmethod
    def load(cls, context: Context, configs: Configurations) -> Weather:
        type = configs.get("type", default="default").lower()
        if type in ["virtual", "default"]:
            return cls(context, configs)

        elif type in ["brightsky", "dwd"]:
            from .dwd import Brightsky
            return Brightsky(context, configs)

        # elif type in ["meteoblue", "nmm"]:
        #     from .meteoblue import Meteoblue
        #     return Meteoblue(context, configs, location)

        raise WeatherException(f"Unknown weather type '{type}' in file: {configs.path}")

    def __init__(self, context: Context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(context, configs, *args, **kwargs)

    # noinspection PyProtectedMember
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
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
            try:
                self._location = self.context.location
                if not isinstance(self._location, Location):
                    raise WeatherException(f"Invalid location type for weather '{self.uuid}': {type(self._location)}")
            except (LocationUnavailableException, AttributeError):
                raise WeatherException(f"Missing location for weather '{self.uuid}'")

    @property
    def location(self) -> Location:
        return self._location

    @property
    def type(self) -> str:
        return self.TYPE


class WeatherException(ComponentException):
    """
    Raise if an error occurred accessing the weather.

    """


class WeatherUnavailableException(ComponentUnavailableException, WeatherException):
    """
    Raise if a configured weather access can not be found.

    """
