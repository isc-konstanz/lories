# -*- coding: utf-8 -*-
"""
lories.data.validation
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt

import pandas as pd
from lories._core.typing import Timezone  # noqa
from lories.core.errors import ResourceError  # noqa


def validate_index(data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
    if not isinstance(data.index, pd.DatetimeIndex):
        try:
            index = pd.to_datetime(data.index)
            if not isinstance(index, pd.DatetimeIndex):
                # Mixed UTC offsets across a DST boundary (e.g. +01:00/+02:00 around Europe/Berlin's
                # spring-forward) yield an object-dtype index: pandas <3 returns it with a FutureWarning,
                # pandas >=3 raises below. Transport timestamps are UTC, so coercing to UTC is correct.
                index = pd.to_datetime(data.index, utc=True)
            data.index = index
        except (pd.errors.ParserError, ValueError) as e:
            try:
                data.index = pd.to_datetime(data.index, utc=True)
            except (pd.errors.ParserError, ValueError):
                raise ResourceError(f"Invalid series, without valid DatetimeIndex: {str(e)}")
    if not data.index.is_unique:
        raise ResourceError(f"Invalid series with non unique index: {data[data.index.duplicated()]}")
    return data


def validate_timezone(data: pd.DataFrame | pd.Series, timezone: Timezone) -> pd.DataFrame | pd.Series:
    if isinstance(data, pd.DatetimeIndex):
        data = data.tz_convert(timezone)
    elif pd.api.types.is_datetime64_dtype(data.values):
        data = data.dt.tz_convert(timezone)
    elif data.map(lambda i: isinstance(i, (pd.Timestamp, dt.datetime))).all():
        data = data.map(lambda i: i.astimezone(timezone))
    return data
