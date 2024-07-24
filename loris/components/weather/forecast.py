# -*- coding: utf-8 -*-
"""
loris.components.weather.forecast
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Optional

import pandas as pd
import pytz as tz
from loris import Configurations
from loris.components import Component
from loris.connectors import Connector
from loris.util import floor_date, to_date, to_timezone


class WeatherForecast(Component):
    TYPE: str = "weather_forecast"
    SECTION: str = "forecast"

    timezone: tz.BaseTzInfo

    interval: int = 60
    offset: int = 0

    def __init__(self, connector, configs: Configurations) -> None:
        super().__init__(connector, configs)
        self.__connector = connector

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        from loris.components.weather import WeatherConnector, WeatherException
        if isinstance(self.__connector, WeatherConnector):
            self.timezone = self.__connector.location.timezone
        elif "timezone" in configs:
            self.timezone = to_timezone(configs.get("timezone"))
        else:
            raise WeatherException(f"Unable to determine timezone for weather forecast '{self.name}'")

        self.interval = configs.get_int("interval", default=WeatherForecast.interval)
        self.offset = configs.get_int("offset", default=WeatherForecast.offset)

    @property
    def type(self) -> str:
        return self.TYPE

    @property
    def connector(self) -> Connector:
        return self.__connector

    def get(
        self,
        start: Optional[pd.Timestamp, dt.datetime, str] = None,
        end: Optional[pd.Timestamp, dt.datetime, str] = None,
        timezone: Optional[tz.BaseTzInfo | str | int | float] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Retrieves the forecasted data for a specified time interval

        :param start:
            the start timestamp for which forecasted data will be looked up for.
            For many applications, passing datetime.datetime.now() will suffice.
        :type start:
            :class:`pandas.Timestamp`, datetime or str

        :param end:
            the end timestamp for which forecasted data will be looked up for.
        :type end:
            :class:`pandas.Timestamp`, datetime or str

        :param timezone:
            the timezone for the timestamps data will be looked up for.
        :type timezone:
            :class:`pytz.BaseTzInfo`, str or number

        :returns:
            the forecasted data, indexed in a specific time interval.

        :rtype:
            :class:`pandas.DataFrame`
        """
        forecast = super().get(start, end, **kwargs)

        # Calculate the available forecast start and end times
        if timezone is None:
            timezone = self.timezone
        else:
            timezone = to_timezone(timezone)
        end = to_date(end, timezone=timezone)
        start = to_date(start, timezone=timezone)
        if start is None:
            start = pd.Timestamp.now(tz=timezone)

        if forecast.empty or forecast.index[0] > start:
            forecast_channels = self.data.filter(lambda c: c.has_logger() and c.has_connector(self.connector.uuid))
            if len(forecast_channels) > 0:
                start_schedule = floor_date(start, self.timezone, freq=f"{self.interval}T")
                start_schedule += pd.Timedelta(minutes=self.offset)
                if start_schedule > start:
                    start_schedule -= pd.Timedelta(minutes=self.interval)

                # TODO: Implement reading of logged forecasts
                # if self.database.exists(start_schedule):
                #     forecast = self.database.read_file(start_schedule).tz_convert(timezone)
                #
                # elif start < pd.Timestamp.now(timezone):
                #     raise WeatherException("Unable to read persisted historic forecast")
                forecast = self._get_range(forecast, start_schedule, end, **kwargs)
        return forecast
