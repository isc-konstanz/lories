# -*- coding: utf-8 -*-
"""
    th-e-core.weather.fcst
    ~~~~~~~~~~~~~~~~~~~~~~
    
    This module provides the :class:`th-e-core.Forecast`, used as reference to calculate a
    photovoltaic installations' generated power. The provided environmental data contains 
    temperatures and horizontal solar irradiation, which can be used, to calculate the 
    effective irradiance on defined, tilted photovoltaic systems.
    
"""
from __future__ import annotations
from abc import abstractmethod

import pytz as tz
import datetime as dt
import pandas as pd
import logging

from ..tools import to_date
from ..configs import Configurations
from ..system import System
from .wx import Weather
from .db import DatabaseWeather

logger = logging.getLogger(__name__)


class WeatherForecast(Weather):

    def __init__(self, system: System, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(system, configs, *args, **kwargs)
        self._context = system

    def get(self,
            start: pd.Timestamp | dt.datetime = dt.datetime.now(),
            end:   pd.Timestamp | dt.datetime = None,
            **kwargs) -> pd.DataFrame:
        """ 
        Retrieves the forecasted data for a specified time interval

        :param start: 
            the start time for which forecasted data will be looked up for.
            For many applications, passing datetime.datetime.now() will suffice.
        :type start: 
            :class:`pandas.Timestamp` or datetime
        
        :param end: 
            the end time for which forecasted data will be looked up for.
            For many applications, passing datetime.datetime.now() will suffice.
        :type end: 
            :class:`pandas.Timestamp` or datetime
        
        :returns: 
            the forecasted data, indexed in a specific time interval.
        
        :rtype: 
            :class:`pandas.DataFrame`
        """
        return self._get_range(self.predict(start, end, **kwargs), start, end)

    @staticmethod
    def _get_range(forecast: pd.DataFrame,
                   start:    pd.Timestamp | dt.datetime,
                   end:      pd.Timestamp | dt.datetime) -> pd.DataFrame:

        if start is not None:
            start = start.astimezone(forecast.index.tz)
        if start is None or start < forecast.index[0]:
            start = forecast.index[0]

        if end is not None:
            end = end.astimezone(forecast.index.tz)
        if end is None or end > forecast.index[-1]:
            end = forecast.index[-1]

        return forecast.loc[start:end, :]

    @abstractmethod
    def predict(self, *args, **kwargs) -> pd.DataFrame:
        pass


class ScheduledForecast(WeatherForecast, DatabaseWeather):

    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        self.interval = configs.getint('General', 'interval', fallback=1440)*3600

    def get(self,
            start: pd.Timestamp | dt.datetime = dt.datetime.now(tz.UTC),
            end:   pd.Timestamp | dt.datetime = None,
            **kwargs) -> pd.DataFrame:

        # Calculate the available forecast start and end times
        interval = self.interval/3600
        timezone = self.context.location.timezone
        start = to_date(start, timezone=timezone)
        end = to_date(end, timezone=timezone)

        start_schedule = start.astimezone(timezone).replace(minute=0, second=0, microsecond=0)
        if start_schedule.hour % interval != 0:
            start_schedule = start_schedule - dt.timedelta(hours=start_schedule.hour % interval)

        if self._database is not None \
                and self._database.exists(start_schedule):
            forecast = self._database.read(start_schedule)

        else:
            forecast = self.predict(start, **kwargs)

            if self._database is not None:
                # Store the retrieved forecast
                self._database.write(forecast, start=start_schedule)

        return self._get_range(forecast, start, end)
