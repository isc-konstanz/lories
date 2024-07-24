# -*- coding: utf-8 -*-
"""
loris.component.weather.dwd.brightsky
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import json
from typing import List, Optional, Tuple

import requests

import numpy as np
import pandas as pd
from loris import Channel, Channels, ChannelState, ConfigurationException, Configurations
from loris.components.weather import Weather, WeatherConnector
from loris.components.weather.dwd._channels import get_channels
from loris.util import ceil_date, floor_date


# noinspection SpellCheckingInspection
class Brightsky(WeatherConnector):
    TYPE: str = "brightsky"

    address: str = "https://api.brightsky.dev/"
    horizon: int = 5

    @property
    def type(self) -> str:
        return self.TYPE

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self.address = configs.get("address", default=Brightsky.address)
        self.horizon = configs.get_int("horizon", default=Brightsky.horizon)
        if -1 > self.horizon > 10:
            raise ConfigurationException(f"Invalid forecast horizon: {self.horizon}")

        if self.forecast.is_enabled():
            self.forecast.data.add(
                id="timestamp_creation",
                name="Creation Timestamp",
                connector=self.uuid,
                horizon=self.horizon,
                source="forecast",
                address="source_first_record",
                value_type=pd.Timestamp,
                primary=True,
                nullable=False,
            )
            for channel in get_channels(connector=self.uuid, horizon=self.horizon, source="forecast"):
                self.forecast.data.add(**channel)

        for channel in get_channels(connector=self.uuid, source=("current", "historical")):
            self.data.add(**channel)

    def has_forecast(self) -> bool:
        return True

    def read(
        self,
        channels: Channels,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> None:
        data, sources = self._request(start, end)
        data_sources = sources.loc[data["source_id"], ["observation_type", "first_record", "last_record"]]
        data_source_columns = ["source_type", "source_first_record", "source_last_record"]
        data_sources.columns = data_source_columns
        data_sources.index = data.index
        data[data_source_columns] = data_sources
        data_source_columns = ["source_id", *data_source_columns]

        for channel in channels:
            if channel.address not in data.columns:
                channel.state = ChannelState.NOT_AVAILABLE
                self._logger.warning("Unable to read nonexisting brightsky column: " + channel.address)
                continue

            channel_sources = channel.source if isinstance(channel.source, (Tuple, List)) else (channel.source,)
            channel_data = data.loc[
                data["source_type"].isin(channel_sources), np.unique([*data_source_columns, channel.address])
            ]
            if channel_data.empty:
                channel.state = ChannelState.UNKNOWN_ERROR
                self._logger.warning(f"Unable to read {self._uuid} channel: {channel.id}")
                continue

            if any(source == "forecast" for source in channel_sources):
                self._parse_forecast(channel_data, channel, start, end)

            elif any(source in ["historical", "current"] for source in channel_sources):
                if not all(t is None for t in [start, end]):
                    self._parse_historical(channel_data, channel, start, end)
                else:
                    self._parse_current(channel_data, channel)
            else:
                channel.state = ChannelState.NOT_AVAILABLE
                self._logger.warning("Unable to read nonexisting brightsky source: " + str(channel.source))

    def _parse_forecast(
        self,
        data: pd.DataFrame,
        channel: Channel,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> None:
        if start is None:
            start = data.index[0]
        else:
            start = floor_date(start, freq="h")
            if end is None:
                end = start + pd.Timedelta(days=channel.horizon)

        if end is None:
            end = data.index[-1]
        else:
            end = ceil_date(end, freq="h")

        timestamps = data["source_first_record"].unique()
        if len(timestamps) > 1:
            channel.state = ChannelState.UNKNOWN_ERROR
            self._logger.warning(f"Unable to read {self._uuid} channel for inconsistend forecast dates: {channel.id}")
            return
        timestamp = timestamps[0].tz_convert(self.location.timezone)

        data = data.loc[start:end, channel.address]
        channel.set(timestamp, data)

    def _parse_current(
        self,
        data: pd.DataFrame,
        channel: Channel
    ) -> None:
        timestamp = data.index[-1].tz_convert(self.location.timezone)
        data = data.loc[timestamp, channel.address]
        channel.set(timestamp, data)

    def _parse_historical(
        self,
        data: pd.DataFrame,
        channel: Channel,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> None:
        timestamp = pd.Timestamp.now(tz=self.location.timezone).floor(freq="s")
        if start is None:
            start = pd.Timestamp(0, tz=self.location.timezone)
        if end is None:
            end = floor_date(timestamp, freq="h")

        data = data.loc[start:end, channel.address]
        channel.set(timestamp, data)

    # noinspection PyPackageRequirements
    def _request(
        self,
        date: Optional[pd.Timestamp, dt.datetime] = None,
        date_last: Optional[pd.Timestamp, dt.datetime] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if date is None:
            date = pd.Timestamp.now(tz=self.location.timezone)
        if date_last is None:
            date_last = date + pd.Timedelta(days=self.horizon)
        parameters = {
            "date": date.strftime("%Y-%m-%d"),
            "last_date": date_last.strftime("%Y-%m-%d"),
            "lat": self.location.latitude,
            "lon": self.location.longitude,
            "tz": self.location.timezone.zone,
        }
        response = requests.get(self.address + "weather", params=parameters)

        if response.status_code != 200:
            raise requests.HTTPError(
                "Response returned with error " + str(response.status_code) + ": " + response.reason
            )

        response_json = json.loads(response.text)

        sources = pd.DataFrame(response_json["sources"])
        sources = sources.set_index("id")
        sources["first_record"] = pd.DatetimeIndex(sources["first_record"])
        sources["last_record"] = pd.DatetimeIndex(sources["last_record"])

        data = pd.DataFrame(response_json["weather"])
        data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
        data = data.set_index("timestamp").tz_convert(self.location.timezone)
        data.index.name = "timestamp"

        hours = pd.Series(data=data.index, index=data.index).diff().bfill().dt.total_seconds() / 3600.0

        # Convert global horizontal irradiance from kWh/m^2 to W/m^2
        data["solar"] = data["solar"] * hours * 1000

        if data[Weather.CLOUD_COVER].isna().any():
            data[Weather.CLOUD_COVER].interpolate(method="linear", inplace=True)

        return data.dropna(how="all", axis="columns"), sources

    def write(self, channels: Channels) -> None:
        raise NotImplementedError("Brightsky connector does not support writing")
