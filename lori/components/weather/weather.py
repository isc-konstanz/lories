# -*- coding: utf-8 -*-
"""
lori.weather
~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import Callable
from typing import Optional, Type, TypeVar

from lori.components import Component, ComponentError, register_component_type
from lori.core import Constant, ResourceError
from lori.core.activator import ActivatorMeta
from lori.core.register import Registry
from lori.location import Location, LocationUnavailableException
from lori.typing import Configurations, ContextArgument


# noinspection PyShadowingBuiltins
def register_weather_type(
    type: str,
    *alias: str,
    factory: Callable[[ContextArgument, Optional[Configurations]], WeatherType] = None,
    replace: bool = False,
) -> Callable[[Type[WeatherType]], Type[WeatherType]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[WeatherType]) -> Type[WeatherType]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


class WeatherMeta(ActivatorMeta):
    def __call__(cls, context: ContextArgument, configs: Configurations, **kwargs) -> WeatherType:
        _type = configs.get("type", default="default").lower()
        _cls = cls._get_class(_type)
        if cls != _cls:
            return _cls(context, configs, **kwargs)

        return super().__call__(context, configs, **kwargs)

    # noinspection PyMethodMayBeStatic, PyShadowingBuiltins
    def _get_class(cls: Type[WeatherType], type: str) -> Type[WeatherType]:
        if type in ["default", "virtual"]:
            return cls
        elif registry.has_type(type):
            registration = registry.from_type(type)
            return registration.type

        raise ResourceError(f"Unknown weather type '{type}'")


# noinspection SpellCheckingInspection
@register_component_type("weather")
class Weather(Component, metaclass=WeatherMeta):
    GHI = Constant(float, "ghi", "Global Horizontal Irradiance", "W/m2")
    DNI = Constant(float, "dni", "Direct Normal Irradiance", "W/m2")
    DHI = Constant(float, "dhi", "Diffuse Horizontal Irradiance", "W/m2")
    TEMP_AIR = Constant(float, "temp_air", "Air Temperature", "°C")
    TEMP_FELT = Constant(float, "temp_felt", "Felt Temperature", "°C")
    TEMP_DEW_POINT = Constant(float, "dew_point", "Dewpoint Temperature", "°C")
    HUMIDITY_REL = Constant(float, "humidity_relative", "Relative Humidity", "%")
    PRESSURE_SEA = Constant(float, "pressure_sea", "Atmospheric Pressure", "hPa")
    WIND_SPEED = Constant(float, "wind_speed", "Wind Speed", "km/h")
    WIND_SPEED_GUST = Constant(float, "wind_speed_gust", "Wind Gust Speed", "km/h")
    WIND_DIRECTION = Constant(float, "wind_direction", "Wind Direction", "°")
    WIND_DIRECTION_GUST = Constant(float, "wind_direction_gust", "Wind Gust Direction", "°")
    CLOUD_COVER = Constant(float, "cloud_cover", "Total Cloud Cover", "%")
    CLOUDS_LOW = Constant(float, "clouds_low", "Low Cloud Cover", "%")
    CLOUDS_MID = Constant(float, "clouds_mid", "Medium Cloud Cover", "%")
    CLOUDS_HIGH = Constant(float, "clouds_high", "High Cloud Cover", "%")
    SUNSHINE = Constant(int, "sunshine", "Sunshine Duration", "min")
    VISIBILITY = Constant(int, "visibility", "Visibility", "m")
    PRECIPITATION = Constant(float, "precipitation", "Precipitation", "mm")
    PRECIPITATION_CONV = Constant(float, "precipitation_convective", "Precipitation Convective", "mm")
    PRECIPITATION_PROB = Constant(float, "precipitation_probability", "Precipitation Probability", "%")
    PRECIPITABLE_WATER = Constant(float, "precipitable_water", "Precipitable Water", "cm")
    SNOW_FRACTION = Constant(float, "snow_fraction", "Snow Fraction", "1/0")

    location: Location

    # noinspection PyProtectedMember
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.localize(configs.get_member(Location.TYPE, defaults={}))

    # noinspection PyUnresolvedReferences
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
                    raise ComponentError(self, f"Invalid location type '{type(self.location)}'")
            except (LocationUnavailableException, AttributeError):
                raise ComponentError(self, "Missing location")


WeatherType = TypeVar("WeatherType", bound=Weather)

registry = Registry[Weather]()
