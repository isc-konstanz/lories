# -*- coding: utf-8 -*-
"""
    loris.components.weather.forecast
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations

import datetime as dt

import pandas as pd
import pytz as tz
from loris import Configurations
from loris.components import Component
from loris.components.weather.connector import WeatherConnector
from loris.util import floor_date, to_date, to_int


class WeatherForecast(Component):
    TYPE: str = "weather_forecast"
    SECTION: str = "forecast"

    connector: WeatherConnector

    def __init__(self, context, connector: WeatherConnector, configs: Configurations) -> None:
        super().__init__(context, configs)
        self.connector = connector

    # noinspection PyProtectedMember
    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        self.interval = to_int(configs.get("interval", default=60))
        self.offset = to_int(configs.get("offset", default=0))

    def get_type(self) -> str:
        return self.TYPE

    def get(
        self,
        start: pd.Timestamp | dt.datetime = pd.Timestamp.now(tz=tz.UTC),
        end: pd.Timestamp | dt.datetime = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Retrieves the forecasted data for a specified time interval

        :param start:
            the start timestamp for which forecasted data will be looked up for.
            For many applications, passing datetime.datetime.now() will suffice.
        :type start:
            :class:`pandas.Timestamp` or datetime

        :param end:
            the end timestamp for which forecasted data will be looked up for.
        :type end:
            :class:`pandas.Timestamp` or datetime

        :returns:
            the forecasted data, indexed in a specific time interval.

        :rtype:
            :class:`pandas.DataFrame`
        """
        # # Calculate the available forecast start and end times
        # timezone = self.location.timezone
        # end = to_date(end, timezone=timezone)
        # start = to_date(start, timezone=timezone)
        #
        # start_schedule = floor_date(start, self.database.timezone, f"{self.interval}T")
        # start_schedule += dt.timedelta(minutes=self.offset)
        # if start_schedule > start:
        #     start_schedule -= dt.timedelta(minutes=self.interval)
        #
        # if self.database.exists(start_schedule):
        #     forecast = self.database.read_file(start_schedule).tz_convert(timezone)
        #
        # elif start < pd.Timestamp.now(timezone):
        #     raise WeatherException("Unable to read persisted historic forecast")
        #
        # else:
        #     forecast = self.predict(start_schedule, **kwargs)
        #
        #     self.database.write_file(forecast, start=start_schedule)
        #
        # return self._get_range(forecast, start_schedule, end)
        return pd.DataFrame()

    # noinspection PyMethodMayBeStatic
    def _get_range(
        self,
        forecast: pd.DataFrame,
        start: pd.Timestamp | dt.datetime,
        end: pd.Timestamp | dt.datetime
    ) -> pd.DataFrame:
        if start is None or start < forecast.index[0]:
            start = forecast.index[0]
        if end is None or end > forecast.index[-1]:
            end = forecast.index[-1]
        return forecast[(forecast.index >= start) & (forecast.index <= end)]
