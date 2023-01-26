# -*- coding: utf-8 -*-
"""
    th-e-core.tools
    ~~~~~~~~~~~~~~~
    
    
"""
import os
import pytz as tz
import datetime as dt
import pandas as pd
from copy import deepcopy
from typing import Union
from configparser import ConfigParser
from pandas.tseries.frequencies import to_offset


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
                     timezone: dt.tzinfo = tz.utc) -> pd.Timestamp:
    if isinstance(date, str):
        import dateutil.parser
        date = dateutil.parser.parse(date)
    if isinstance(date, dt.datetime):
        date = pd.Timestamp(date)

    if isinstance(date, pd.Timestamp):
        if date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
            return date.tz_localize(timezone)
        else:
            return date.tz_convert(timezone)


def floor_date(date: Union[dt.datetime, pd.Timestamp, str],
               timezone: dt.tzinfo = None) -> Union[dt.datetime,
                                                    pd.Timestamp]:
    if timezone is not None:
        date = convert_timezone(date, timezone)
    return date.replace(hour=0, minute=0, second=0, microsecond=0)


def ceil_date(date: Union[dt.datetime, pd.Timestamp, str],
              timezone: dt.tzinfo = None) -> Union[dt.datetime,
                                                   pd.Timestamp]:
    if timezone is not None:
        date = convert_timezone(date, timezone)
    return date.replace(hour=23, minute=59, second=59, microsecond=999999)


def resample_data(data: pd.DataFrame, seconds: int) -> pd.DataFrame:
    resampled = pd.DataFrame()
    resampled.index.name = 'time'
    for column, series in deepcopy(data).iteritems():
        series = _resample_series(series, seconds)

        resampled = pd.concat([resampled, series.to_frame()], axis=1)
    return resampled.dropna(how='all')


def _resample_series(data: pd.Series, seconds: int) -> pd.Series:
    resampled = data.resample('{}s'.format(seconds), closed='right')
    if data.name.endswith('_energy'):
        data = resampled.last()
    else:
        data = resampled.mean()
    data.index += to_offset('{}s'.format(seconds))
    return data


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
