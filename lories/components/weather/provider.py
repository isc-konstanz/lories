# -*- coding: utf-8 -*-
"""
lories.components.weather.provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module provides the :class:`lories.components.weather.provider.WeatherProvider`, used as
reference to calculate e.g. photovoltaic installations generated power. The provided
environmental data contains temperatures and horizontal solar irradiation, which can be used,
to calculate the effective irradiance on defined, tilted photovoltaic systems.

"""

from __future__ import annotations

from typing import Optional

from lories.components.weather import Weather, WeatherPredictor
from lories.typing import Configurations, ContextArgument


# noinspection SpellCheckingInspection
class WeatherProvider(Weather):

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        predictor_configs = configs.get_member(WeatherPredictor.TYPE, ensure_exists=True)
        predictor_configs.set("key", "forecast", replace=False)
        self.predictors.add(WeatherPredictor(self, configs))

    @property
    def forecast(self) -> WeatherPredictor:
        return self.predictors.get_first(WeatherPredictor)
