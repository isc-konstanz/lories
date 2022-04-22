# -*- coding: utf-8 -*-
"""
    th-e-core.tools
    ~~~~~~~~~~~~~~~
    
    
"""
import os
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


def convert_timezone(date: Union[dt.datetime, pd.Timestamp, str],
                     timezone: dt.tzinfo = tz.utc) -> Union[dt.datetime,
                                                            pd.Timestamp]:
    if isinstance(date, str):
        import dateutil.parser
        date = dateutil.parser.parse(date)

    if isinstance(date, dt.datetime):
        if date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
            return timezone.localize(date)
        elif date.tzinfo != timezone:
            return date.astimezone(timezone)

    if isinstance(date, pd.Timestamp):
        if date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
            return date.tz_localize(timezone)
        else:
            return date.tz_convert(timezone)


def floor_date(date: Union[dt.datetime, pd.Timestamp, str],
               timezone: dt.tzinfo = tz.utc) -> Union[dt.datetime,
                                                      pd.Timestamp]:

    date = convert_timezone(date, timezone)
    return date.replace(hour=0, minute=0, second=0, microsecond=0)  # , nanosecond=0)


def ceil_date(date: Union[dt.datetime, pd.Timestamp, str],
              timezone: dt.tzinfo = tz.utc) -> Union[dt.datetime,
                                                     pd.Timestamp]:

    date = convert_timezone(date, timezone)
    return date.replace(hour=23, minute=59, second=59, microsecond=999)  # , nanosecond=999)


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
