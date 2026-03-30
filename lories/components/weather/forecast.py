# -*- coding: utf-8 -*-
"""
lories.components.weather.forecast
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Optional

import pandas as pd
import pytz as tz
from lories.components.weather import Weather
from lories.core.configs.parameters import Parameter
from lories.core.errors import ResourceError
from lories.core.typing import Component, Configurations
from lories.location import Location
from lories.util import floor_date, to_date, to_timezone


class WeatherForecast(Weather):
    """
    A weather forecast sub-component that stores and retrieves predicted meteorological data on a
    configurable schedule. It is owned by a WeatherProvider and shares its geographic location.
    Forecast data is periodically fetched and cached, with historical entries available through the
    data logger for gap-filling when the in-memory buffer does not cover the requested time range.
    """

    TYPE: str = "forecast"

    interval = Parameter(type=int, default=60, desc="Forecast schedule interval in minutes")
    offset = Parameter(type=int, default=0, desc="Forecast schedule offset in minutes")

    interval: int
    offset: int

    @classmethod
    def _assert_context(cls, context: Component):
        from lories.components.weather import WeatherProvider

        if context is None or not isinstance(context, WeatherProvider):
            raise ResourceError(f"Invalid '{cls.__name__}' context: {type(context)}")
        return super()._assert_context(context)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

    def localize(self, configs: Configurations) -> None:
        # Do nothing, as context was already validated as WeatherProvider, that does have a location
        pass

    # noinspection PyUnresolvedReferences
    @property
    def location(self) -> Location:
        return self.context.location

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
        forecast = self.data.to_frame(unique=False)

        # Calculate the available forecast start and end times
        if timezone is None:
            timezone = self.location.timezone
        else:
            timezone = to_timezone(timezone)
        end = to_date(end, timezone=timezone)
        start = to_date(start, timezone=timezone)
        if start is None:
            start = pd.Timestamp.now(tz=timezone)

        if forecast.empty or start < forecast.index[0] or end > forecast.index[-1]:
            start_schedule = floor_date(start, self.location.timezone, freq=f"{self.interval}T")
            start_schedule += pd.Timedelta(minutes=self.offset)
            if start_schedule > start:
                start_schedule -= pd.Timedelta(minutes=self.interval)

            logged = self.data.from_logger(start=start, end=end, unique=False)
            if not logged.empty:
                forecast = logged if forecast.empty else forecast.combine_first(logged)

        return self._get_range(forecast, start, end, **kwargs)
