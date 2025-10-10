# -*- coding: utf-8 -*-
"""
lori.components.weather.dwd.provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Collection, Dict

import pandas as pd
from lori import Constant
from lori.components.weather import Weather, WeatherForecast, WeatherProvider, register_weather_type
from lori.components.weather.dwd import Brightsky
from lori.typing import Configurations

CHANNELS = [
    Weather.GHI,
    Weather.TEMP_AIR,
    Weather.TEMP_DEW_POINT,
    Weather.PRESSURE_SEA,
    Weather.WIND_SPEED,
    Weather.WIND_SPEED_GUST,
    Weather.WIND_DIRECTION,
    Weather.CLOUD_COVER,
    Weather.SUNSHINE,
    Weather.VISIBILITY,
    Weather.PRECIPITATION,
    Weather.PRECIPITATION_PROB,
    Constant(str, "condition", "Condition"),
    Constant(str, "icon", "Icon"),
]

CHANNEL_ADDRESS_ALIAS = {
    Weather.GHI: "solar",
    Weather.TEMP_AIR: "temperature",
    Weather.PRESSURE_SEA: "pressure_msl",
    Weather.WIND_SPEED_GUST: "wind_gust_speed",
    Weather.WIND_DIRECTION_GUST: "wind_gust_direction",
}

CHANNEL_AGGREGATE_ALIAS = {
    Weather.SUNSHINE: "sum",
    Weather.PRECIPITATION: "sum",
    Weather.WIND_SPEED_GUST: "max",
    "condition": None,
    "icon": None,
}


# noinspection SpellCheckingInspection
@register_weather_type("dwd", "brightsky")
class DeutscherWetterDienst(WeatherProvider):
    # noinspection PyUnresolvedReferences
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        connector = Brightsky(self, self.location, key="brightsky")
        self.connectors.add(connector)

        if self.forecast.is_enabled() and isinstance(self.forecast, WeatherForecast):
            self.forecast.data.add(
                key="timestamp_creation",
                name="Creation Timestamp",
                connector=connector.id,
                address="source_first_record",
                source="forecast",
                type=pd.Timestamp,
                aggregate="last",
                logger={
                    "primary": True,
                    "nullable": False,
                },
            )
            for channel in build_channels(connector=connector.id, source="forecast"):
                self.forecast.data.add(**channel)

        for channel in build_channels(connector=connector.id, source="current, historical"):
            if channel["key"] not in [Weather.PRECIPITATION_PROB]:
                self.data.add(**channel)


def build_channels(**custom: Any) -> Collection[Dict[str, Any]]:
    channels = []
    for channel in CHANNELS:
        configs = channel.to_dict()
        if channel.key in CHANNEL_ADDRESS_ALIAS:
            configs["address"] = CHANNEL_ADDRESS_ALIAS[channel.key]
        else:
            configs["address"] = channel.key
        if configs["type"] == str:  # noqa: E721
            configs["length"] = 32
        if channel.key in CHANNEL_AGGREGATE_ALIAS:
            configs["aggregate"] = CHANNEL_AGGREGATE_ALIAS[channel.key]
        else:
            configs["aggregate"] = "mean"
        configs.update(custom)
        channels.append(configs)
    return channels
