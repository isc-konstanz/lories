# -*- coding: utf-8 -*-
"""
    loris.component.weather.dwd._configs
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from typing import Dict, Any
from collections import OrderedDict
from loris.components.weather import Weather, WEATHER_NAMES as CHANNEL_NAMES

CHANNEL_IDS = [
    Weather.GHI,
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
    Weather.PRECIPITATION_PROB,
    'condition',
    'icon'
]

CHANNEL_ADDRESS_ALIAS = {
    Weather.GHI:                 'solar',
    Weather.TEMP_AIR:            'temperature',
    Weather.PRESSURE_SEA:        'pressure_msl',
    Weather.WIND_SPEED_GUST:     'wind_gust_speed',
    Weather.WIND_DIRECTION_GUST: 'wind_gust_direction'
}

CHANNEL_TYPE_DEFAULT = float
CHANNEL_TYPES = {
    Weather.SUNSHINE: int,
    Weather.VISIBILITY: int,
    Weather.PRECIPITATION_PROB: int,
    'condition': str,
    'icon': str
}


def _parse_name(channel_id: str) -> str:
    return channel_id.replace('_', ' ').title() if channel_id not in CHANNEL_NAMES else CHANNEL_NAMES[channel_id]


def _parse_address(channel_id: str) -> str:
    return channel_id if channel_id not in CHANNEL_ADDRESS_ALIAS else CHANNEL_ADDRESS_ALIAS[channel_id]


def _parse_value_type(channel_id: str) -> type:
    return CHANNEL_TYPE_DEFAULT if channel_id not in CHANNEL_TYPES else CHANNEL_TYPES[channel_id]


def _parse_channel(channel_id: str) -> Dict[str, Any]:
    channel = {
        'id': channel_id,
        'name': _parse_name(channel_id),
        'address': _parse_address(channel_id),
        'value_type': _parse_value_type(channel_id)
    }
    if channel['value_type'] == str:
        channel['value_length'] = 16
    return channel


def parse_defaults(uuid: str) -> Dict[str, Any]:
    channel_defaults = OrderedDict({
        'connector': uuid
    })
    for channel_id in CHANNEL_IDS:
        channel_defaults[channel_id] = _parse_channel(channel_id)
    return channel_defaults
