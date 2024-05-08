# -*- coding: utf-8 -*-
"""
    loris.component.weather.dwd._configs
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from typing import Dict, Any
from loris.components.weather import Weather, WEATHER as CHANNEL_NAMES


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


def parse_defaults(uuid: str) -> Dict[str, Any]:
    channels_defaults = {
        'reader': {
            'connector': uuid
        }
    }
    for channel_id in CHANNEL_IDS:
        channel_name = channel_id.replace('_', ' ').title() \
            if channel_id not in CHANNEL_NAMES else CHANNEL_NAMES[channel_id]
        channel_address = channel_id \
            if channel_id not in CHANNEL_ADDRESS_ALIAS else CHANNEL_ADDRESS_ALIAS[channel_id]
        channels_defaults[channel_id] = {
            'name': channel_name,
            'address': channel_address
        }
    return channels_defaults
