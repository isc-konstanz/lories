# -*- coding: utf-8 -*-
"""
loris.util
~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import dateutil.parser
import re
from copy import copy
from dateutil.relativedelta import relativedelta
from pydoc import locate
from typing import Any, Callable, Collection, Dict, List, Mapping, Optional, Tuple, Type, TypeVar

import numpy as np
import pandas as pd
import pytz as tz
from loris.core import Context, ResourceException
from pandas.tseries.frequencies import to_offset

# noinspection SpellCheckingInspection
INVALID_CHARS = "'!@#$%^&?*;:,./\\|`´+~=- "

C = TypeVar("C", bound=Context)
V = TypeVar("V")


# noinspection PyShadowingBuiltins
def get_context(object: Any, type: Type[C] | Collection[Type[C]]) -> Optional[C]:
    _context = object
    if _context is None:
        return _context
    while not isinstance(_context, type):
        try:
            _context = _context.context
        except AttributeError:
            raise ResourceException(f"Invalid context type: {object.__class__}")
    return _context


# noinspection PyShadowingBuiltins, PyShadowingNames
def get_variables(
    obj: Any,
    include: Optional[Type[V]] = object,
    exclude: Optional[Type | Tuple[Type, ...]] = None,
) -> List[V]:
    def _is_type(o) -> bool:
        return isinstance(o, include) and (exclude is None or not isinstance(o, exclude))

    if isinstance(obj, Collection):
        return [o for o in obj if _is_type(o)]
    return list(get_members(obj, lambda attr, member: _is_type(member)).values())


# noinspection PyShadowingBuiltins
def get_members(
    obj: Any,
    filter: Optional[Callable] = None,
    private: bool = False,
) -> Dict[str, Any]:
    members = dict()
    processed = set()
    for attr in dir(obj):
        try:
            member = getattr(obj, attr)
            # Handle duplicate attr
            if attr in processed:
                raise AttributeError
        except (AttributeError, ResourceException):
            continue
        if (
            (private or "__" not in attr)
            and (filter is None or filter(attr, member))
            and member not in members.values()
        ):
            members[attr] = member
        processed.add(attr)
    return dict(sorted(members.items()))


def update_recursive(configs: Dict[str, Any], update: Mapping[str, Any], replace: bool = True) -> Dict[str, Any]:
    for k, v in update.items():
        if isinstance(v, Mapping):
            if k not in configs.keys():
                configs[k] = {}
            configs[k] = update_recursive(configs[k], v, replace)
        elif k not in configs or replace:
            configs[k] = v
    return configs


# noinspection PyUnresolvedReferences, PyShadowingBuiltins, PyShadowingNames
def is_type(series: pd.Series, *type: str) -> bool:
    # TODO: Introduce Unit or "Nature" constant, to differentiate better
    series_name = series.name.split("_")
    series_suffix_start = -1 if len(series_name) < 3 else -2
    series_suffix = series_name[series_suffix_start:]
    return any(t in series_suffix for t in type)


def infer_resample_method(series: pd.Series) -> str:
    if is_type(series, "state", "state", "mode", "code"):
        return "max"
    elif is_type(series, "timestamp", "time", "progress", "energy"):
        return "last"
    else:
        return "mean"


# noinspection PyUnresolvedReferences
def resample(data: pd.DataFrame | pd.Series, resolution: int) -> pd.DataFrame:
    resampled = []

    for column, series in (data if isinstance(data, pd.DataFrame) else data.to_frame()).items():
        series = series.dropna()
        series = resample_series(series, resolution, infer_resample_method(series))

        resampled.append(series)

    resampled = pd.concat(resampled, axis="columns")
    resampled.index.name = data.index.name
    return resampled.dropna(how="all")


# noinspection PyUnresolvedReferences
def resample_series(
    data: pd.Series,
    resolution: int,
    method: str,
    offset: pd.Timedelta = None,
) -> pd.Series:
    kwargs = {}
    if offset is not None:
        kwargs["offset"] = offset
    index = copy(data.index)
    freq = index.freq
    if freq is None and len(index) > 2:
        freq = pd.tseries.frequencies.to_offset(pd.infer_freq(index))
    if freq is None or freq.delta < pd.Timedelta(seconds=resolution):
        # FIXME: AttributeError: 'NoneType' object has no attribute 'delta'
        resampled = data.resample(f"{int(resolution)}s", closed="right", **kwargs)
        if method == "mean":
            data = resampled.mean()
        elif method == "max":
            # The max method is commonly used vor integer state/state/code values
            data = resampled.max().astype(int)
        elif method == "sum":
            data = resampled.sum()
        elif method == "last":
            data = resampled.last()
        else:
            raise ValueError(f"Unknown resampling method: {method}")

        data.index += to_offset(f"{int(resolution)}s")
    return data[(data.index >= index[0]) & (data.index <= index[-1])]


def derive_by_hours(data: pd.Series) -> pd.Series:
    """
    Derive a data series by hours.

    Parameters
    ----------
    data : pd.Series
        Series with the data to be derived

    Returns
    ----------
    fixed: pd.Series
        Series with the derived data

    """
    delta_value = data.iloc[:].astype("float64").diff()

    delta_index = pd.Series(delta_value.index, index=delta_value.index)
    delta_index = (delta_index - delta_index.shift(1)) / np.timedelta64(1, "h")

    return pd.Series(delta_value / delta_index, index=data.index).dropna()


def convert_timezone(
    date: dt.datetime | pd.Timestamp | str,
    timezone: dt.tzinfo = tz.UTC,
) -> Optional[pd.Timestamp]:
    if date is None:
        return None
    if isinstance(date, str):
        date = dateutil.parser.parse(date)
    if isinstance(date, dt.datetime):
        date = pd.Timestamp(date)

    if isinstance(date, pd.Timestamp):
        if date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
            return date.tz_localize(timezone)
        else:
            return date.tz_convert(timezone)
    else:
        raise TypeError(f"Unable to convert date of type {type(date)}")


def slice_range(
    start: dt.datetime | pd.Timestamp | str,
    end: dt.datetime | pd.Timestamp | str,
    timezone: dt.tzinfo = None,
    freq: str = "D",
    **kwargs,
) -> List[Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]]:
    start = to_date(start, timezone, **kwargs)
    end = to_date(end, timezone, **kwargs)

    if start is None or end is None:
        return [(start, end)]

    freq_delta = to_timedelta(freq)

    def _next(timestamp: pd.Timestamp) -> pd.Timestamp:
        _timestamp = floor_date(timestamp + freq_delta, freq=freq)
        if _timestamp > timestamp:
            return _timestamp
        # Handle daylight savings
        return floor_date(timestamp + freq_delta * 2, freq=freq)

    ranges = []
    range_start = start
    if floor_date(start, freq=freq) == start:
        range_start += pd.Timedelta(seconds=1)
    range_end = min(_next(start), end)
    ranges.append((range_start, range_end))

    while range_end < end:
        range_start = _next(range_start)
        range_end = min(_next(range_start), end)
        ranges.append((range_start + pd.Timedelta(seconds=1), range_end))
    return ranges


def floor_date(
    date: dt.datetime | pd.Timestamp | str,
    timezone: dt.tzinfo = None,
    freq: str = "D",
) -> Optional[pd.Timestamp]:
    if date is None:
        return None
    if timezone is None:
        timezone = date.tzinfo
    date = convert_timezone(date, timezone)
    freq = parse_freq(freq)
    if freq in ["Y", "M"]:
        return date.tz_localize(None).to_period(freq).to_timestamp().tz_localize(timezone, ambiguous=True)
    elif any([freq.endswith(f) for f in ["D", "h", "min", "s"]]):
        return date.tz_localize(None).floor(freq).tz_localize(timezone, ambiguous=True)
    else:
        raise ValueError(f"Invalid frequency: {freq}")


def ceil_date(
    date: dt.datetime | pd.Timestamp | str,
    timezone: dt.tzinfo = None,
    freq: str = "D",
) -> Optional[pd.Timestamp]:
    date = floor_date(date, timezone, freq)
    if date is None:
        return None

    return date + to_timedelta(freq) - dt.timedelta(microseconds=1)


# noinspection PyShadowingBuiltins
def to_date(
    date: Optional[str | int | dt.datetime | pd.Timestamp],
    timezone: dt.tzinfo = None,
    format: str = "%d.%m.%Y",
) -> Optional[pd.Timestamp]:
    if date is None:
        return None

    def _convert_timezone(_date: pd.Timestamp) -> pd.Timestamp:
        if timezone is not None:
            _date = convert_timezone(_date, timezone)
        return _date

    if issubclass(type(date), dt.datetime):
        return _convert_timezone(date)
    if isinstance(date, str):
        return _convert_timezone(pd.Timestamp(dt.datetime.strptime(date, format)))
    if isinstance(date, int):
        return _convert_timezone(pd.Timestamp(dt.datetime.fromtimestamp(date)))

    raise TypeError(f"Invalid date type: {type(date)}")


def to_timezone(timezone: Optional[str | int | float | tz.BaseTzInfo]) -> Optional[tz.BaseTzInfo]:
    if timezone is None:
        return None
    if isinstance(timezone, tz.BaseTzInfo):
        return timezone
    if isinstance(timezone, str):
        try:
            return tz.timezone(timezone)

        except tz.UnknownTimeZoneError:
            # Handle the case where the timezone is not recognized
            if timezone == "CEST":
                return tz.FixedOffset(2 * 60)

            # Handle offset time strings
            return pd.to_datetime(timezone, format="%z").tzinfo

    if isinstance(timezone, (int, float)):
        return tz.FixedOffset(timezone * 60)

    raise TypeError(f"Invalid timezone type: {type(timezone)}")


def to_timedelta(freq: str) -> relativedelta | pd.Timedelta:
    freq_val = "".join(s for s in freq if s.isnumeric())
    freq_val = int(freq_val) if len(freq_val) > 0 else 1
    freq = parse_freq(freq)
    if freq == "Y":
        return relativedelta(years=freq_val)
    elif freq == "M":
        return relativedelta(months=freq_val)
    elif freq.endswith("D"):
        return pd.Timedelta(days=freq_val)
    elif freq.endswith("h"):
        return pd.Timedelta(hours=freq_val)
    elif freq.endswith("min"):
        return pd.Timedelta(minutes=freq_val)
    elif freq.endswith("s"):
        return pd.Timedelta(seconds=freq_val)
    else:
        raise ValueError(f"Invalid frequency: {freq}")


def to_float(value: str | float) -> Optional[float]:
    if value is None:
        return None
    if type(value) == float:
        return value
    if isinstance(value, str) or issubclass(type(value), float):
        return float(value)
    raise TypeError(f"Expected str or float, not: {type(value)}")


def to_int(value: str | int) -> Optional[int]:
    if value is None:
        return None
    if type(value) == int:
        return value
    if isinstance(value, str) or issubclass(type(value), int):
        return int(value)
    raise TypeError(f"Expected str or int, not: {type(value)}")


def to_bool(value: str | bool) -> Optional[bool]:
    if value is None:
        return None
    if type(value) == bool:
        return value
    if isinstance(value, str):
        if value.lower() in ["true", "yes", "y"]:
            return True
        if value.lower() in ["false", "no", "n"]:
            return False
    if issubclass(type(value), int):
        return bool(value)
    raise TypeError(f"Invalid bool type: {type(value)}")


# noinspection SpellCheckingInspection
def parse_freq(freq: str) -> Optional[str]:
    value = "".join(s for s in freq if s.isnumeric())
    value = int(value) if len(value) > 0 else 1
    if freq.upper() == "Y":
        return "Y"
    elif freq.upper() == "M":
        return "M"
    elif freq.lower().endswith(("d", "day", "days")):
        return f"{value}D"
    elif freq.lower().endswith(("h", "hour", "hours")):
        return f"{value}h"
    elif freq.lower().endswith(("t", "min", "mins")):
        return f"{value}min"
    elif freq.lower().endswith(("s", "sec", "secs")):
        return f"{value}s"
    else:
        raise ValueError(f"Invalid frequency: {freq}")


# noinspection PyShadowingBuiltins
def parse_type(t: str | Type, default: Type = None) -> Type:
    if t is None:
        if default is None:
            raise ValueError("Invalid NoneType")
        return default
    if isinstance(t, str):
        t = locate(t)
    if not isinstance(t, type):
        raise TypeError(f"Invalid type: {type(t)}")
    return t


def parse_name(name: str) -> str:
    return " ".join(
        [
            s.upper() if len(s) <= 3 else s.title()
            for s in re.sub(r"[,._\- ]", " ", re.sub(r"[^0-9A-Za-zäöüÄÖÜß%&;:()_.\- ]+", "", name)).split()
        ]
    )


# noinspection PyShadowingBuiltins
def parse_key(id: str) -> str:
    for c in INVALID_CHARS:
        id = id.replace(c, "_")
    return re.sub(r"\W", "", id).lower()
