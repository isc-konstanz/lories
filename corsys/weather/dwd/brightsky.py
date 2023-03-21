# -*- coding: utf-8 -*-
"""
    corsys.weather.nmm
    ~~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations

import json
import pytz as tz
import datetime as dt
import pandas as pd
import requests

from corsys.tools import to_date

from io import StringIO
from ...configs import Configurations
from ...system import System
from ..fcst import ScheduledForecast
from ..base import Weather


# noinspection SpellCheckingInspection
class Brightsky(ScheduledForecast):

    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)

        # TODO: Add sanity check
        self.address = configs.get('Brightsky', 'address', fallback='https://api.brightsky.dev/')
        self.horizon = configs.getint('Brightsky', 'horizon', fallback=5)
        if -1 > self.horizon > 10:
            raise ValueError(f"Invalid forecast horizon: {self.horizon}")

        self._variables = {
            Weather.TEMP_AIR:            'temperature',
            Weather.PRESSURE_SEA:        'pressure_msl',
            Weather.WIND_SPEED_GUST:     'wind_gust_speed',
            Weather.WIND_DIRECTION_GUST: 'wind_gust_direction'
        }

        self._variables_output = [
            Weather.TEMP_AIR,
            Weather.TEMP_DEW_POINT,
            Weather.HUMIDITY_REL,
            Weather.PRESSURE_SEA,
            Weather.WIND_SPEED,
            Weather.WIND_SPEED_GUST,
            Weather.WIND_DIRECTION,
            Weather.WIND_DIRECTION_GUST,
            Weather.CLOUD_COVER,
            Weather.SUNSHINE,
            Weather.VISIBILITY,
            Weather.PRECIPITATION,
            'condition',
            'icon'
        ]

    def __activate__(self, system: System, configs: Configurations) -> None:
        super().__activate__(system, configs)
        self.location = system.location

    def get(self,
            start: dt.datetime | pd.Timestamp = pd.Timestamp.now(tz.UTC),
            end:   dt.datetime | pd.Timestamp = None,
            **kwargs) -> pd.DataFrame:

        timezone = self.location.timezone
        today = pd.Timestamp.now(timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        start = to_date(start, timezone=timezone)
        end = to_date(end, timezone=timezone)
        if start >= today:
            return super().get(start, end, **kwargs)
        elif end is None:
            end = today - dt.timedelta(days=1)

        # TODO: Check and read data from database first and request and write data if not existing
        forecast = self._request(start, end)
        return self._get_range(forecast, start, end)

    def predict(self, date: pd.Timestamp) -> pd.DataFrame:
        return self._request(date)

    # noinspection PyPackageRequirements
    def _request(self,
                 date:      pd.Timestamp,
                 date_last: pd.Timestamp = None) -> pd.DataFrame:
        """
        Submits a query to the meteoblue servers and
        converts the CSV response to a pandas DataFrame.
        
        Returns
        -------
        data : DataFrame
            column names are the weather model's variable names.
        """
        if date_last is None:
            date_last = date + dt.timedelta(days=self.horizon)
        parameters = {
            'date': date.strftime('%Y-%m-%d'),
            'last_date': date_last.strftime('%Y-%m-%d'),
            'lat': self.location.latitude,
            'lon': self.location.longitude,
            'tz': self.location.timezone.zone
        }
        response = requests.get(self.address + 'weather', params=parameters)

        if response.status_code != 200:
            raise requests.HTTPError("Response returned with error " + str(response.status_code) + ": " +
                                     response.reason)

        data = json.loads(response.text)
        data = pd.DataFrame(data['weather'])
        data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True)
        data = data.set_index('timestamp').tz_convert(self.location.timezone)
        data.index.name = 'time'

        if data[Weather.CLOUD_COVER].isna().any():
            data[Weather.CLOUD_COVER].interpolate(method='linear', inplace=True)

        return self._rename(data)[self._variables_output]
