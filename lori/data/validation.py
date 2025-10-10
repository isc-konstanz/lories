# -*- coding: utf-8 -*-
"""
lori.data.validation
~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt

import pandas as pd
from lori._core.typing import Timezone  # noqa
from lori.core.errors import ResourceError  # noqa


def validate_index(data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
    if not isinstance(data.index, pd.DatetimeIndex):
        try:
            data.index = pd.to_datetime(data.index)
        except (pd.errors.ParserError, ValueError) as e:
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
