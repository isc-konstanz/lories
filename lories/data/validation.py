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


def validate_timezone(data: pd.DataFrame | pd.Series, timezone: Timezone, convert: bool = True) -> pd.DataFrame | pd.Series:
    # Timezone-naive data is always localized to the passed timezone; already aware data is
    # converted only when `convert` is set, otherwise its timezone is kept as delivered.
    if isinstance(data, pd.DatetimeIndex):
        if data.tz is None:
            data = data.tz_localize(timezone)
        elif convert:
            data = data.tz_convert(timezone)
    elif pd.api.types.is_datetime64_any_dtype(data):
        if data.dt.tz is None:
            data = data.dt.tz_localize(timezone)
        elif convert:
            data = data.dt.tz_convert(timezone)
    elif data.map(lambda i: isinstance(i, (pd.Timestamp, dt.datetime))).all():
        def _validate(i):
            if i.tzinfo is None:
                return pd.Timestamp(i).tz_localize(timezone)
            return i.astimezone(timezone) if convert else i

        data = data.map(_validate)
    return data
