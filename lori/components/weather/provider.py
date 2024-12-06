# -*- coding: utf-8 -*-
"""
lori.components.weather.component
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module provides the :class:`lori.components.weather.weather.Weather`, used as reference to
calculate e.g. photovoltaic installations generated power. The provided environmental data
contains temperatures and horizontal solar irradiation, which can be used, to calculate the
effective irradiance on defined, tilted photovoltaic systems.

"""

from __future__ import annotations

from functools import wraps
from typing import Optional, Type

from lori.components import Component, register_component_type
from lori.components.weather import WeatherForecast
from lori.core import ActivatorMeta, Configurations, Context
from lori.weather import Weather, WeatherException

TYPE: str = "weather"


class WeatherProviderMeta(ActivatorMeta):
    def __call__(cls, context: Context | WeatherProvider, configs: Configurations, **kwargs) -> WeatherProvider:
        _type = configs.get("type", default="default").lower()
        _cls = cls._get_class(_type)
        if cls != _cls:
            return _cls(context, configs, **kwargs)

        return super().__call__(context, configs, **kwargs)

    # fmt: off
    # noinspection PyShadowingBuiltins
    def _get_class(cls: Type[WeatherProvider], type: str) -> Type[WeatherProvider]:
        if type in ["virtual", "default"]:
            return cls

        elif type in ["dwd", "brightsky"]:
            from lori.components.weather.dwd import DeutscherWetterDienst
            return DeutscherWetterDienst

        raise WeatherException(f"Unknown weather type '{type}'")
    # fmt: on


# noinspection SpellCheckingInspection
@register_component_type(TYPE)
class WeatherProvider(Component, Weather, metaclass=WeatherProviderMeta):
    __forecast: WeatherForecast

    def __init__(
        self,
        context: Context | Component,
        configs: Optional[Configurations] = None,
        **kwargs,
    ) -> None:
        super().__init__(context=context, configs=configs, **kwargs)
        forecast_configs = configs.get_section(WeatherForecast.SECTION, ensure_exists=True)
        forecast_configs.set("key", "forecast", replace=False)
        self.__forecast = WeatherForecast(self, forecast_configs)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

    @wraps(configure, updated=())
    def _do_configure(self, configs: Configurations, *args, **kwargs) -> None:
        super()._do_configure(configs)
        if self.__forecast.is_enabled():
            self.__forecast.configure(configs.get_section(WeatherForecast.SECTION))
            if len(self.__forecast.data) == 0:
                self.__forecast.configs.enabled = False

    def activate(self) -> None:
        super().activate()
        if self.forecast.is_enabled():
            self.forecast.activate()

    def deactivate(self) -> None:
        super().deactivate()
        if self.forecast.is_enabled():
            self.forecast.deactivate()

    @property
    def forecast(self) -> WeatherForecast:
        return self.__forecast
