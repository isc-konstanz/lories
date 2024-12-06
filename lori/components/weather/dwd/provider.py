# -*- coding: utf-8 -*-
"""
lori.connector.weather.dwd.component
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Collection, Dict

import pandas as pd
from lori import Configurations, Weather
from lori.components import register_component_type
from lori.components.weather import WeatherForecast, WeatherProvider
from lori.components.weather.constants import WEATHER as CHANNEL_NAMES
from lori.components.weather.dwd import Brightsky

CHANNEL_IDS = [
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
    "condition",
    "icon",
]

CHANNEL_ADDRESS_ALIAS = {
    Weather.GHI: "solar",
    Weather.TEMP_AIR: "temperature",
    Weather.PRESSURE_SEA: "pressure_msl",
    Weather.WIND_SPEED_GUST: "wind_gust_speed",
    Weather.WIND_DIRECTION_GUST: "wind_gust_direction",
}

CHANNEL_TYPE_DEFAULT = float
CHANNEL_TYPES = {
    Weather.SUNSHINE: int,
    Weather.VISIBILITY: int,
    Weather.PRECIPITATION_PROB: int,
    "condition": str,
    "icon": str,
}


# noinspection SpellCheckingInspection
@register_component_type("dwd", "brightsky")
class DeutscherWetterDienst(WeatherProvider):
    # noinspection PyUnresolvedReferences
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        connector = Brightsky(self, key="brightsky")
        self.connectors.add(connector)

        if self.forecast.is_enabled() and isinstance(self.forecast, WeatherForecast):
            self.forecast.data.add(
                key="timestamp_creation",
                name="Creation Timestamp",
                connector=connector.id,
                address="source_first_record",
                source="forecast",
                type=pd.Timestamp,
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


def _parse_name(key: str) -> str:
    return key.replace("_", " ").title() if key not in CHANNEL_NAMES else CHANNEL_NAMES[key]


def _parse_address(key: str) -> str:
    return key if key not in CHANNEL_ADDRESS_ALIAS else CHANNEL_ADDRESS_ALIAS[key]


def _parse_type(key: str) -> type:
    return CHANNEL_TYPE_DEFAULT if key not in CHANNEL_TYPES else CHANNEL_TYPES[key]


def _build_channel(key: str, **channel: Any) -> Dict[str, Any]:
    channel["key"] = key
    channel["name"] = _parse_name(key)
    channel["address"] = _parse_address(key)
    channel["type"] = _parse_type(key)
    if channel["type"] == str:  # noqa: E721
        channel["length"] = 32
    return channel


def build_channels(**channel: Any) -> Collection[Dict[str, Any]]:
    channels = []
    for channel_key in CHANNEL_IDS:
        channels.append(_build_channel(channel_key, **channel))
    return channels
