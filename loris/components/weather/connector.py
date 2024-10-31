# -*- coding: utf-8 -*-
"""
loris.components.weather.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from typing import Optional

import pandas as pd
from loris.components.weather import Weather, WeatherException, WeatherForecast, WeatherMeta
from loris.connectors import Connector, ConnectorMeta
from loris.core import Configurations, Context
from loris.data.context import DataContext
from loris.util import get_context


class WeatherConnectorMeta(WeatherMeta, ConnectorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, context: Context, *args, **kwargs):
        connector = super().__call__(context, *args, **kwargs)
        connector_context = get_context(context, DataContext).connectors
        connector_context._add(connector)
        return connector


class WeatherConnector(Weather, Connector, metaclass=WeatherConnectorMeta):
    _forecast: WeatherForecast = None

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        if self.has_forecast():
            section = configs.get_section(WeatherForecast.SECTION, ensure_exists=True)
            section.set("key", "forecast", replace=False)
            self._forecast = WeatherForecast(self, section)

    def _on_configure(self, configs: Configurations) -> None:
        super()._on_configure(configs)
        if self.has_forecast():
            self._forecast.configure(configs.get_section(WeatherForecast.SECTION))

    def activate(self) -> None:
        super().activate()
        if self.has_forecast():
            self._forecast.activate()

    def deactivate(self) -> None:
        super().deactivate()
        if self.has_forecast():
            self._forecast.deactivate()

    @property
    def context(self):
        return super(Weather, self).context

    @property
    def forecast(self) -> WeatherForecast:
        if not self.has_forecast() or self._forecast is None:
            raise WeatherException(f"Weather '{self.name}' has no forecast configured")
        return self._forecast

    @abstractmethod
    def has_forecast(self) -> bool:
        pass

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
        weather = super().get(start, end, **kwargs)
        if self.has_forecast():
            weather_forecast = self.forecast.get(start, end, **kwargs)
            if not weather_forecast.empty:
                weather = weather.combine_first(weather_forecast)
        return weather
