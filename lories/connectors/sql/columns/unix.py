# -*- coding: utf-8 -*-
"""
lories.connectors.sql.columns.timestamp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from sqlalchemy.types import INTEGER

import numpy as np
import pandas as pd
import pytz as tz
from lories.connectors.sql.columns import Column, ColumnType


# noinspection PyShadowingBuiltins
class UnixTimestampColumn(Column):
    inherit_cache: bool = True

    def __init__(
        self,
        name: str,
        type: ColumnType = INTEGER,
        *args,
        default: Optional[Any] = None,
        nullable: bool = False,
        **kwargs,
    ) -> None:
        # if default is None and not nullable:
        #     default = "(UNIX_TIMESTAMP())"
        super().__init__(
            name,
            type,
            *args,
            default=default,
            nullable=nullable,
            primary_key=True,
            **kwargs,
        )

    # noinspection PyUnresolvedReferences
    def validate(self, data: Any) -> Any:
        if isinstance(data, pd.Series):
            data = data.dt.tz_convert(tz.UTC).view(np.int64) // 10**9
        elif isinstance(data, pd.DatetimeIndex):
            data = data.tz_convert(tz.UTC).view(np.int64) // 10**9
        elif isinstance(data, pd.Timestamp):
            epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
            data = data.tz_convert(tz.UTC)
            data = int((data - epoch).total_seconds())
        else:
            epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
            data = data.astimezone(tz.UTC)
            data = int((data - epoch).total_seconds())
        return super().validate(data)
