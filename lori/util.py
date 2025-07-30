# -*- coding: utf-8 -*-
"""
lori.util
~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import dateutil.parser
import re
from dateutil.relativedelta import relativedelta
from pydoc import locate
from typing import Any, Callable, Collection, Dict, List, Mapping, Optional, Tuple, Type, TypeVar

import tzlocal

import numpy as np
import pandas as pd
import pytz as tz
from lori.core import ResourceException
from lori.typing import TimestampType, TimezoneType

# noinspection SpellCheckingInspection
INVALID_CHARS = "'!@#$%^&?*;:,./\\|`´+~=- "

C = TypeVar("C")  # , bound=Context)
V = TypeVar("V")


# noinspection PyShadowingBuiltins
def get_context(object: Any, type: Type[C] | Collection[Type[C]]) -> Optional[C]:
    _context = object
    if _context is None or isinstance(_context, type):
        return _context
    while not isinstance(_context, type):
        try:
            _context = _context.context
        except AttributeError:
            raise ResourceException(f"Invalid context type: {object.__class__}")
    return _context


# noinspection PyShadowingBuiltins
def get_includes(type: Type) -> List[str]:
    includes = []
    for _type in type.mro():
        if not hasattr(_type, "INCLUDES"):
            continue
        _includes = getattr(_type, "INCLUDES")
        if not isinstance(_includes, Collection) or isinstance(_includes, str):
            continue
        for include in _includes:
            if include not in includes:
                includes.append(include)
    return includes


# noinspection PyShadowingBuiltins, PyShadowingNames
def get_variables(
    object: Any,
    include: Optional[Type[V]] = object,
    exclude: Optional[Type | Tuple[Type, ...]] = None,
) -> List[V]:
    def _is_type(o) -> bool:
        return isinstance(o, include) and (exclude is None or not isinstance(o, exclude))

    if isinstance(object, Collection):
        return [o for o in object if _is_type(o)]
    return list(get_members(object, lambda attr, member: _is_type(member)).values())


# noinspection PyShadowingBuiltins
def get_members(
    object: Any,
    filter: Optional[Callable] = None,
    private: bool = False,
) -> Dict[str, Any]:
    members = dict()
    processed = set()
    for attr in dir(object):
        try:
            member = getattr(object, attr)
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
    for key, update_value in update.items():
        if isinstance(update_value, Mapping):
            update_map = configs.get(key, {})
            if isinstance(update_map, Mapping):
                update_map = update_recursive(update_map, update_value, replace=replace)
            elif not replace:
                continue
            configs[key] = update_map
        elif key not in configs.keys() or replace:
            configs[key] = update_value
    return configs


def convert_timezone(
    date: Optional[TimestampType | str],
    timezone: Optional[TimezoneType] = None,
) -> Optional[pd.Timestamp]:
    if date is None:
        return None
    if isinstance(date, str):
        date = dateutil.parser.parse(date)
    if isinstance(date, dt.datetime):
        date = pd.Timestamp(date)

    if isinstance(date, pd.Timestamp):
        if timezone is None:
            timezone = to_timezone(tzlocal.get_localzone_name())
        if date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
            return date.tz_localize(timezone)
        else:
            return date.tz_convert(timezone)
    else:
        raise TypeError(f"Unable to convert date of type {type(date)}")


def slice_range(
    start: Optional[TimestampType | str],
    end: Optional[TimestampType | str],
    timezone: Optional[TimezoneType] = None,
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
    date: Optional[TimestampType | str],
    timezone: Optional[TimezoneType] = None,
    freq: str = "D",
) -> Optional[pd.Timestamp]:
    if date is None:
        return None
    if timezone is None:
        timezone = date.tzinfo
    date = convert_timezone(date, timezone)
    freq = parse_freq(freq)
    if any([freq.endswith(f) for f in ["Y", "M", "W"]]):
        return date.tz_localize(None).to_period(freq).to_timestamp().tz_localize(timezone, ambiguous=True)
    elif any([freq.endswith(f) for f in ["D", "h", "min", "s"]]):
        return date.tz_localize(None).floor(freq).tz_localize(timezone, ambiguous=True)
    else:
        raise ValueError(f"Invalid frequency: {freq}")


def ceil_date(
    date: Optional[TimestampType | str],
    timezone: Optional[TimezoneType] = None,
    freq: str = "D",
) -> Optional[pd.Timestamp]:
    date = floor_date(date, timezone, freq)
    if date is None:
        return None

    return date + to_timedelta(freq) - pd.Timedelta(microseconds=1)


# noinspection PyShadowingBuiltins
def to_date(
    date: Optional[TimestampType | str | int],
    timezone: Optional[TimezoneType] = None,
    format: Optional[str] = None,
) -> Optional[pd.Timestamp]:
    if date is None:
        return None

    if issubclass(type(date), dt.datetime):
        return convert_timezone(date, timezone)
    if isinstance(date, int):
        return convert_timezone(pd.Timestamp(date, unit="s"), timezone)
    if isinstance(date, str):
        if format is None:
            return convert_timezone(pd.Timestamp(date), timezone)
        return convert_timezone(pd.Timestamp(dt.datetime.strptime(date, format)), timezone)

    raise TypeError(f"Invalid date type: {type(date)}")


def to_timezone(timezone: Optional[TimezoneType | str | int | float]) -> Optional[TimezoneType]:
    if timezone is None:
        return None
    if isinstance(timezone, (tz.BaseTzInfo, dt.tzinfo)):
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
    if freq.endswith("Y"):
        return relativedelta(years=freq_val)
    elif freq.endswith("M"):
        return relativedelta(months=freq_val)
    elif freq.endswith("W"):
        return relativedelta(weeks=freq_val)
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


def is_float(value: str | float) -> bool:
    def _is_numeric(s: str) -> bool:
        return s.replace(".", "").replace(",", "").replace("e+", "").replace("e-", "").replace("e", "").isnumeric()

    if (
        issubclass(type(value), (np.integer, int))
        or issubclass(type(value), (np.floating, float))
        or (isinstance(value, str) and _is_numeric(value))
    ):
        return True
    return False


def to_float(value: str | float) -> Optional[float]:
    if value is None:
        return None
    if type(value) == float:  # noqa E721
        return value
    if is_float(value):
        return float(value)
    raise TypeError(f"Expected str, int or float, not: {type(value)}")


def is_int(value: str | int) -> bool:
    if (
        (issubclass(type(value), (np.floating, float)) and int(value) == value)
        or issubclass(type(value), (np.integer, int))
        or (isinstance(value, str) and value.isnumeric())
    ):
        return True
    return False


def to_int(value: str | int) -> Optional[int]:
    if value is None:
        return None
    if type(value) == int:  # noqa E721
        return value
    if is_int(value):
        return int(value)
    raise TypeError(f"Expected str, float or int, not: {type(value)}")


def is_bool(value: str | bool) -> bool:
    if issubclass(type(value), (np.bool, bool, np.integer, int)) or (
        isinstance(value, str) and any(value.lower() == b for b in ["true", "false", "yes", "no", "y", "n"])
    ):
        return True
    return False


def to_bool(value: str | bool) -> Optional[bool]:
    if value is None:
        return None
    if type(value) == bool:  # noqa E721
        return value
    if isinstance(value, str):
        if value.lower() in ["true", "yes", "y"]:
            return True
        if value.lower() in ["false", "no", "n"]:
            return False
    if issubclass(type(value), (np.bool, bool, int)):
        return bool(value)
    raise TypeError(f"Invalid bool type: {type(value)}")


# noinspection SpellCheckingInspection
def parse_freq(freq: str) -> Optional[str]:
    if freq is None:
        return None
    match = re.fullmatch(r"\s*(\d*)\s*([a-zA-Z]+)\s*", freq)
    if not match:
        raise ValueError(f"Invalid frequency format: '{freq}'")

    number_part, unit_part = match.groups()
    value = int(number_part) if number_part else 1

    def _parse_freq(suffix: str) -> str:
        return str(value) + suffix if value > 1 else suffix

    if unit_part.lower() in ["y", "year", "years"]:
        return _parse_freq("Y")
    elif unit_part.lower() in ["m", "month", "months"]:
        return _parse_freq("M")
    elif unit_part.lower() in ["w", "week", "weeks"]:
        return _parse_freq("W")
    elif unit_part.lower() in ["d", "day", "days"]:
        return _parse_freq("D")
    elif unit_part.lower() in ["h", "hour", "hours"]:
        return _parse_freq("h")
    elif unit_part.lower() in ["t", "min", "mins"]:
        return _parse_freq("min")
    elif unit_part.lower() in ["s", "sec", "secs"]:
        return _parse_freq("s")
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
def validate_key(id: str) -> str:
    for c in INVALID_CHARS:
        id = id.replace(c, "_")
    return re.sub(r"\W", "", id).lower()
