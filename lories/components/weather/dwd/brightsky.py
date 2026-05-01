# -*- coding: utf-8 -*-
"""
lories.connector.weather.dwd.brightsky
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import json
from typing import Optional, Tuple

import requests

import numpy as np
import pandas as pd
from lories.components.weather import Weather
from lories.connectors import Connector, register_connector_type
from lories.core.configs.parameters import Parameter
from lories.location import Location
from lories.typing import Configurations, Resources, Timestamp


@register_connector_type("brightsky")
class Brightsky(Connector):
    """
    Connector for the Bright Sky API, an open REST interface that re-publishes Deutscher Wetterdienst (DWD)
    open weather data without requiring registration or API keys. It serves observations, current conditions
    and forecasts for any geographic location, returning hourly records covering solar irradiance, temperature,
    wind, precipitation, cloud cover and related parameters. This connector queries the ``/weather`` endpoint
    for the location bound to the parent weather component, converts global horizontal irradiance from
    kWh/m² to W/m², interpolates missing cloud cover values, and groups records by source type
    (``forecast``, ``current``, ``historical``) so resources can subscribe to the appropriate slice.
    """

    address = Parameter(key="address", type=str, default="https://api.brightsky.dev/", desc="Brightsky API base URL")
    horizon = Parameter(key="horizon", type=int, default=10, min=-1, max=10, desc="Forecast horizon (days)")

    location: Location
    address: str
    horizon: int

    def __init__(self, context: Weather, location: Location, **kwargs) -> None:
        super().__init__(context, context.configs.get_member("brightsky", defaults={}), **kwargs)
        self.location = location

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

    def read(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame:
        response, sources = self._request(start, end)
        response_sources = sources.loc[response["source_id"], ["observation_type", "first_record", "last_record"]]
        response_source_columns = ["source_type", "source_first_record", "source_last_record"]
        response_sources.columns = response_source_columns
        response_sources.index = response.index
        response[response_source_columns] = response_sources

        data = []
        for source, source_resources in resources.groupby("source"):
            source_columns = [r.address for r in source_resources if r.address in response.columns]
            source_data = response.loc[
                response["source_type"].isin(s.strip() for s in source.split(",")),
                np.unique(["source_id", "source_first_record", "source_last_record"] + source_columns),
            ]
            if source_data.empty:
                self._logger.warning(f"Unable to read {self._id} channels: {[r.id for r in source_resources]}")
                continue

            source_start = start if start is not None else min(source_data["source_first_record"].unique())
            source_end = end if end is not None else max(source_data["source_last_record"].unique())

            source_data = source_data.rename(columns={r.address: r.id for r in source_resources})

            if source == "forecast":
                source_end = source_start + pd.Timedelta(days=self.horizon)

            elif any(s in ["historical", "current"] for s in source.split(",")):
                if all(t is None for t in [start, end]):
                    source_start = source_end

            data.append(
                source_data.loc[
                    source_start:source_end, [r.id for r in source_resources if r.id in source_data.columns]
                ]
            )
        return pd.concat(data, axis="index")

    # noinspection PyPackageRequirements
    def _request(
        self,
        date: Optional[Timestamp] = None,
        date_last: Optional[Timestamp] = None,
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
        sources["first_record"] = pd.to_datetime(sources["first_record"], utc=True)
        sources["last_record"] = pd.to_datetime(sources["last_record"], utc=True)

        data = pd.DataFrame(response_json["weather"])
        data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
        data = data.set_index("timestamp").tz_convert(self.location.timezone)
        data.index.name = "timestamp"

        hours = pd.Series(data=data.index, index=data.index).diff().bfill().dt.total_seconds() / 3600.0

        # Convert global horizontal irradiance from kWh/m^2 to W/m^2
        data["solar"] = data["solar"] * hours * 1000

        if data[Weather.CLOUD_COVER].isna().any():
            data[Weather.CLOUD_COVER] = data[Weather.CLOUD_COVER].interpolate(method="linear")

        return data.dropna(how="all", axis="columns"), sources

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Brightsky connector does not support writing")
