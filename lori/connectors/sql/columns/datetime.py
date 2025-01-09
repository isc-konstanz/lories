# -*- coding: utf-8 -*-
"""
lori.connectors.sql.columns.datetime
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional, TypeVar

from sqlalchemy.sql import func
from sqlalchemy.types import DATE, DATETIME, TIME, TIMESTAMP

import pandas as pd
import pytz as tz
from lori.connectors.sql.columns import Column, ColumnType
from lori.core import ResourceException

DatetimeType = TypeVar("DatetimeType", pd.DatetimeIndex, pd.Series, pd.Timestamp, dt.datetime, dt.date, dt.time)


class DatetimeColumn(Column):
    inherit_cache: bool = True

    timezone: tz.BaseTzInfo

    # noinspection PyShadowingBuiltins, PyTypeChecker
    def __init__(
        self,
        name: str,
        type: ColumnType,
        default: Optional[Any] = None,
        nullable: bool = True,
        timezone: tz.BaseTzInfo = tz.UTC,
        **kwargs,
    ) -> None:
        if not is_datetime(type):
            raise ResourceException(f"Invalid data type for DatetimeColumn: {type}")

        if default is None and not nullable:
            if type == TIMESTAMP:
                default = func.current_timestamp()
            elif type == DATETIME:
                default = func.now()
            elif type == DATE:
                default = func.current_date()
            elif type == TIME:
                default = func.current_time()
        super().__init__(name, type(timezone), default=default, nullable=nullable, **kwargs)
        self.timezone = timezone

    # noinspection PyUnresolvedReferences
    def validate(self, data: Any) -> Any:
        if not is_datetime(self.type):
            return data
        if (
            data is None
            or not isinstance(data, (pd.Series, pd.DatetimeIndex, dt.datetime, dt.date, dt.time))
            or (
                isinstance(data, pd.Series)
                and not data.map(lambda i: isinstance(i, (dt.datetime, dt.date, dt.time))).all()
            )
        ):
            raise ResourceException(f"Unable to prepare datetime index from '{type(data)}' data: {data}")

        if isinstance(data, (pd.DatetimeIndex, pd.Timestamp)):
            data = data.tz_convert(self.timezone).tz_localize(None)

        elif isinstance(data, pd.Series):
            data = data.dt.tz_convert(self.timezone).dt.tz_localize(None)
        else:
            data = data.astimezone(self.timezone).replace(tzinfo=None)

        if type == DATE:
            if isinstance(data, (pd.DatetimeIndex, pd.Series)):
                data = data.date
            else:
                data = data.date()
        elif type == TIME:
            if isinstance(data, (pd.DatetimeIndex, pd.Series)):
                data = data.time
            else:
                data = data.time()

        return super().validate(data)


# noinspection PyShadowingBuiltins, SpellCheckingInspection
def is_datetime(type: ColumnType) -> bool:
    datetimes = (
        TIME,
        DATE,
        DATETIME,
        TIMESTAMP,
    )
    return type in datetimes or isinstance(type, datetimes)
