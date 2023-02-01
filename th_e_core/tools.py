# -*- coding: utf-8 -*-
"""
    th-e-core.tools
    ~~~~~~~~~~~~~~~
    
    
"""
import os
import time
import pytz as tz
import datetime as dt
import numpy as np
import pandas as pd
from copy import copy, deepcopy
from typing import Union
from pandas.tseries.frequencies import to_offset


def resample_data(data: pd.DataFrame, seconds: int) -> pd.DataFrame:
    resampled = pd.DataFrame()
    resampled.index.name = 'time'
    for column, series in deepcopy(data).iteritems():
        series = _resample_series(series, seconds)

        resampled = pd.concat([resampled, series.to_frame()], axis=1)
    return resampled.dropna(how='all')


def _resample_series(data: pd.Series, seconds: int) -> pd.Series:
    index = copy(data.index)
    resampled = data.resample('{}s'.format(seconds), closed='right')
    if data.name.endswith('_energy'):
        data = resampled.last()
    else:
        data = resampled.mean()
    data.index += to_offset('{}s'.format(seconds))

    return data[(data.index >= index[0]) & (data.index <= index[-1])]


def derive_power(data: pd.Series) -> pd.Series:
    """
    Derive the power from energy for a Series.

    Parameters
    ----------
    data : pd.Series
        Series with the energy data

    Returns
    ----------
    fixed: pd.Series
        Series with the derived power data

    """
    delta_energy = data.iloc[:].astype('float64').diff()

    delta_index = pd.Series(delta_energy.index, index=delta_energy.index)
    delta_index = (delta_index - delta_index.shift(1)) / np.timedelta64(1, 'h')

    data_power = pd.Series(delta_energy / delta_index, index=data.index)
    data_power.name = data.name.replace("_energy", "_power")

    return data_power.dropna()


def convert_timezone(date: Union[dt.datetime, pd.Timestamp, str],
                     timezone: dt.tzinfo = tz.UTC) -> pd.Timestamp:
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
    else:
        raise ConversionException(f"Unable to convert date of type {type(date)}")


def floor_date(date: Union[dt.datetime, pd.Timestamp, str],
               timezone: dt.tzinfo = None) -> Union[dt.datetime, pd.Timestamp]:
    if timezone is not None:
        date = convert_timezone(date, timezone)
    return date.replace(hour=0, minute=0, second=0, microsecond=0)


def ceil_date(date: Union[dt.datetime, pd.Timestamp, str],
              timezone: dt.tzinfo = None) -> Union[dt.datetime, pd.Timestamp]:
    if timezone is not None:
        date = convert_timezone(date, timezone)
    return date.replace(hour=23, minute=59, second=59, microsecond=999999)


def to_date(date: Union[str, int, dt.datetime, pd.Timestamp],
            timezone: dt.tzinfo = None,
            format: str = '%d.%m.%Y') -> pd.Timestamp:
    if date is None:
        return date

    if isinstance(time, str):
        date = pd.Timestamp(dt.datetime.strptime(date, format))
    if isinstance(time, int):
        date = pd.Timestamp(dt.datetime.fromtimestamp(date))
    if timezone is not None:
        date = convert_timezone(date, timezone)
    return date


def to_float(value: Union[str, float]) -> float:
    if isinstance(value, str):
        return float(value)
    return value


def to_int(value: Union[str, int]) -> int:
    if isinstance(value, str):
        return int(value)
    return value


def to_bool(value: Union[str, bool]) -> bool:
    if isinstance(value, str):
        return value.lower() in ['true', 'yes', 'y']
    return value


class ConversionException(Exception):
    """
    Raise if a conversion failed

    """
    pass
