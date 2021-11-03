# -*- coding: utf-8 -*-
"""
    th-e-core.tools
    ~~~~~~~~~~~~~~~
    
    
"""
import os
from unittest.mock import _patch_dict

import pytz as tz
import datetime as dt
import pandas as pd
from typing import Union
from configparser import ConfigParser


def join_path(configs: ConfigParser,
              key: str,
              path: str,
              section: str = 'General') -> str:
    if configs.has_option(section, key):
        path = configs.get(section, key)

    if "~" in path:
        path = os.path.expanduser(path)

    if not os.path.isabs(path) and \
            configs.has_option(section, 'root_dir'):
        base = configs.get(section, 'root_dir')
        path = os.path.join(base, path)

    return path


def floor_date(date: Union[dt.datetime, pd.Timestamp],
               timezone: dt.tzinfo = tz.utc) -> dt.datetime:

    # TODO: verify if localized and if timezone differs
    date = date.tz_convert(timezone)
    return date.replace(hour=0, minute=0, second=0, microsecond=0, nanosecond=0)


def ceil_date(date: dt.datetime,
              timezone: dt.tzinfo = tz.utc) -> dt.datetime:

    # TODO: verify if localized and if timezone differs
    date = date.tz_convert(timezone)
    return date.replace(hour=23, minute=59, second=59, microsecond=999, nanosecond=999)


def to_bool(v: Union[str, bool]) -> bool:
    if isinstance(v, str):
        return v.lower() == 'true'

    return v


def to_float(v: Union[str, float]) -> float:
    if isinstance(v, str):
        return float(v)

    return v


def to_int(v: Union[str, int]) -> int:
    if isinstance(v, str):
        return int(v)

    return v
