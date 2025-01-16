# -*- coding: utf-8 -*-
"""
lori.connectors.sql.index
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from enum import Enum
from typing import Collection, Optional

from sqlalchemy.types import DATE, DATETIME, INTEGER, TIME, TIMESTAMP, TypeEngine

import pytz as tz
from lori.connectors.sql.columns import Column, DatetimeColumn, UnixTimestampColumn


# noinspection PyShadowingBuiltins
class DatetimeIndexType(Enum):
    NONE = ()
    DATE_AND_TIME = (DATE, TIME)
    DATETIME = (DATETIME,)
    TIMESTAMP = (TIMESTAMP,)
    TIMESTAMP_UNIX = (INTEGER,)

    @classmethod
    def get(cls, type: str) -> DatetimeIndexType:
        for t in cls:
            if t.name == type:
                return t
        raise ValueError(type)

    def __contains__(self, type: TypeEngine):
        return any(type == t or isinstance(type, t) for t in self.value)

    def columns(self, name: Optional[str] = None) -> Collection[Column]:
        columns = []
        for column_type in self.value:
            column_name = name if name is not None else self.name.lower()
            if column_type == DatetimeIndexType.TIMESTAMP_UNIX:
                column = UnixTimestampColumn(column_name, column_type)
            else:
                column = DatetimeColumn(column_name, column_type, timezone=tz.UTC, nullable=False, primary_key=True)
            columns.append(column)
        return columns
