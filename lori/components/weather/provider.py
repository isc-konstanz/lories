# -*- coding: utf-8 -*-
"""
lori.components.weather.provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module provides the :class:`lori.components.weather.provider.WeatherProvider`, used as
reference to calculate e.g. photovoltaic installations generated power. The provided
environmental data contains temperatures and horizontal solar irradiation, which can be used,
to calculate the effective irradiance on defined, tilted photovoltaic systems.

"""

from __future__ import annotations

from typing import Optional

from lori.components.weather import Weather, WeatherForecast
from lori.typing import Configurations, ContextArgument


# noinspection SpellCheckingInspection
class WeatherProvider(Weather):
    __forecast: WeatherForecast

    # noinspection PyTypeChecker
    def __init__(
        self,
        context: ContextArgument,
        configs: Optional[Configurations] = None,
        **kwargs,
    ) -> None:
        super().__init__(context=context, configs=configs, **kwargs)
        forecast_configs = configs.get_section(WeatherForecast.TYPE, ensure_exists=True)
        forecast_configs.set("key", "forecast", replace=False)
        self.__forecast = WeatherForecast(self, forecast_configs)

    def _at_configure(self, configs: Configurations) -> None:
        super()._at_configure(configs)
        if self.__forecast.is_enabled():
            self.__forecast.configure(configs.get_section(WeatherForecast.TYPE, ensure_exists=True))

    def _on_configure(self, configs: Configurations) -> None:
        super()._on_configure(configs)
        if self.__forecast.is_enabled() and len(self.__forecast.data) == 0:
            self.__forecast.configs.enabled = False

    def activate(self) -> None:
        super().activate()
        if self.__forecast.is_enabled():
            self.__forecast.activate()

    def deactivate(self) -> None:
        super().deactivate()
        if self.__forecast.is_active():
            self.__forecast.deactivate()

    @property
    def forecast(self) -> WeatherForecast:
        return self.__forecast
