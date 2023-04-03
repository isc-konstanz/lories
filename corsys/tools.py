# -*- coding: utf-8 -*-
"""
    corsys.tools
    ~~~~~~~~~~~~
    
    
"""
import pytz as tz
import datetime as dt
import numpy as np
import pandas as pd
from copy import copy, deepcopy
from typing import Optional, Union
from pandas.tseries.frequencies import to_offset
from dateutil.relativedelta import relativedelta


def resample_data(data: pd.DataFrame, seconds: int) -> pd.DataFrame:
    resampled = pd.DataFrame()
    resampled.index.name = 'time'
    for column, series in deepcopy(data).items():
        series = _resample_series(series, seconds)

        resampled = pd.concat([resampled, series.to_frame()], axis=1)
    return resampled.dropna(how='all')


def _resample_series(data: pd.Series, seconds: int) -> pd.Series:
    index = copy(data.index)
    resampled = data.resample('{}s'.format(seconds), closed='right')
    if str(data.name).endswith('_energy'):
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
    data_power.name = str(data.name).replace("_energy", "_power")

    return data_power.dropna()


def convert_timezone(date: Union[dt.datetime, pd.Timestamp, str],
                     timezone: dt.tzinfo = tz.UTC) -> Optional[pd.Timestamp]:
    if date is None:
        return None
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
               timezone: dt.tzinfo = None,
               freq: str = 'D') -> Optional[pd.Timestamp]:
    if date is None:
        return None
    if timezone is None:
        timezone = date.tzinfo
    date = convert_timezone(date, timezone)

    if freq in ['Y', 'M']:
        return date.tz_localize(None).to_period(freq).to_timestamp().tz_localize(timezone)
    elif any([freq.endswith(f) for f in ['D', 'H', 'T']]):
        return date.tz_localize(None).floor(freq).tz_localize(timezone)
    else:
        raise ValueError(f"Invalid frequency: {freq}")


def ceil_date(date: Union[dt.datetime, pd.Timestamp, str],
              timezone: dt.tzinfo = None,
              freq: str = 'D') -> Optional[pd.Timestamp]:
    date = floor_date(date, timezone, freq)
    if date is None:
        return None

    return date + to_timedelta(freq) - dt.timedelta(microseconds=1)


# noinspection PyShadowingBuiltins
def to_date(date: Union[str, int, dt.datetime, pd.Timestamp],
            timezone: dt.tzinfo = None,
            format: str = '%d.%m.%Y') -> Optional[pd.Timestamp]:
    if date is None:
        return None

    if isinstance(date, str):
        date = pd.Timestamp(dt.datetime.strptime(date, format))
    if isinstance(date, int):
        date = pd.Timestamp(dt.datetime.fromtimestamp(date))
    if timezone is not None:
        date = convert_timezone(date, timezone)
    return date


def to_timedelta(freq: str) -> relativedelta:
    freq_val = freq[:-1]
    freq_val = int(freq_val) if len(freq_val) > 0 and freq_val.isnumeric() else 1
    freq = freq[-1:]
    if freq == 'Y':
        return relativedelta(years=freq_val)
    elif freq == 'M':
        return relativedelta(months=freq_val)
    elif freq.endswith('D'):
        return relativedelta(days=freq_val)
    elif freq.endswith('H'):
        return relativedelta(hours=freq_val)
    elif freq.endswith('T'):
        return relativedelta(minutes=freq_val)
    else:
        raise ValueError(f"Invalid frequency: {freq}")


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
