# -*- coding: utf-8 -*-
"""
    loris.weather.forecast
    ~~~~~~~~~~~~~~~~~~~~~~
    
    This module provides the :class:`loris.weather.WeatherForecast`, used as reference to
    calculate e.g. photovoltaic installations generated power. The provided environmental data
    contains temperatures and horizontal solar irradiation, which can be used, to calculate the
    effective irradiance on defined, tilted photovoltaic systems.
    
"""
from __future__ import annotations

import datetime as dt
import pandas as pd

from loris.util import to_int, to_date, floor_date
from loris import Configurations
from loris.components.weather import WeatherBase, WeatherConnector, WeatherException


class WeatherForecast(WeatherBase):

    SECTION: str = 'forecast'

    def __init__(self, context, connector: WeatherConnector, configs: Configurations) -> None:
        super().__init__(context, configs)
        self._connector = connector

    # noinspection PyProtectedMember
    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        self.interval = to_int(configs.get('interval', default=60))
        self.delay = to_int(configs.get('delay', default=0))

    def get(self,
            start: pd.Timestamp | dt.datetime = dt.datetime.now(),
            end:   pd.Timestamp | dt.datetime = None,
            **kwargs) -> pd.DataFrame:
        """ 
        Retrieves the forecasted data for a specified time interval

        :param start: 
            the start timestamp for which forecasted data will be looked up for.
            For many applications, passing datetime.datetime.now() will suffice.
        :type start: 
            :class:`pandas.Timestamp` or datetime
        
        :param end: 
            the end timestamp for which forecasted data will be looked up for.
            For many applications, passing datetime.datetime.now() will suffice.
        :type end: 
            :class:`pandas.Timestamp` or datetime
        
        :returns: 
            the forecasted data, indexed in a specific time interval.
        
        :rtype: 
            :class:`pandas.DataFrame`
        """
        # Calculate the available forecast start and end times
        timezone = self.location.timezone
        end = to_date(end, timezone=timezone)
        start = to_date(start, timezone=timezone)

        start_schedule = floor_date(start, self.database.timezone, f"{self.interval}T")
        start_schedule += dt.timedelta(minutes=self.delay)
        if start_schedule > start:
            start_schedule -= dt.timedelta(minutes=self.interval)

        if self.database.exists(start_schedule):
            forecast = self.database.read_file(start_schedule).tz_convert(timezone)

        elif start < pd.Timestamp.now(timezone):
            raise WeatherException("Unable to read_file persisted historic forecast")

        else:
            forecast = self.predict(start_schedule, **kwargs)

            self.database.write_file(forecast, start=start_schedule)

        return self._get_range(forecast, start_schedule, end)

    # noinspection PyMethodMayBeStatic
    def _get_range(self,
                   forecast: pd.DataFrame,
                   start:    pd.Timestamp | dt.datetime,
                   end:      pd.Timestamp | dt.datetime) -> pd.DataFrame:
        if start is None or start < forecast.index[0]:
            start = forecast.index[0]
        if end is None or end > forecast.index[-1]:
            end = forecast.index[-1]
        return forecast[(forecast.index >= start) & (forecast.index <= end)]

    def predict(self, *args, **kwargs) -> pd.DataFrame:
        pass
