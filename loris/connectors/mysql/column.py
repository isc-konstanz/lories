# -*- coding: utf-8 -*-
"""
loris.connectors.mysql.column
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any, AnyStr, Optional, Type

from mysql.connector import FieldType

import pandas as pd
import pytz as tz
from loris.core import ConfigurationException


class MySqlColumn:
    primary: bool
    nullable: bool

    name: str

    type: str
    type_length: int

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        name: str,
        type: AnyStr | Type | int,
        type_length: Optional[int] = None,
        nullable: bool = True,
        primary: bool = False,
    ):
        self.primary = primary
        self.nullable = nullable

        self.name = name
        self.type = self._parse_type(type, type_length)
        if self.type == "TIMESTAMP" and primary and not nullable:
            self.type += " DEFAULT CURRENT_TIMESTAMP"
        self.type_length = type_length

        self._logger = logging.getLogger(__name__)
        self._logger.debug(f"Configured {'PRIMARY ' if self.primary else ''}COLUMN {self}")

    def __repr__(self) -> str:
        column = f"`{self.name}` {self.type}"
        if self.type_length is not None:
            column += f"({self.type_length})"
        if not self.nullable:
            column += " NOT NULL"
        return column

    # noinspection PyShadowingBuiltins
    @staticmethod
    def _parse_type(type: AnyStr | Type | int, type_length: Optional[int] = None) -> str:
        if isinstance(type, str):
            return type.upper()
        elif isinstance(type, int):
            type_info = FieldType.get_info(type)
            if type_info is None:
                raise ConfigurationException(f"Unknown MySQL data type code: {type}")

            return type_info[1]

        if type == str:  # noqa: E721
            return "VARCHAR" if type_length is not None else "STRING"
        elif type in [pd.Timestamp, dt.datetime]:
            return "TIMESTAMP"
        elif type == int:  # noqa: E721
            return "INT"
        elif type == float:  # noqa: E721
            return "FLOAT"

        raise ConfigurationException(f"Unknown MySQL data type: {type}")

    def encode(self, value: Any) -> Any:
        if pd.isna(value):
            return None
        if self.type.startswith("TIMESTAMP"):
            return value.astimezone(tz.UTC)
            # return value.astimezone(tz.UTC).strftime('%Y-%m-%d %H:%M:%S')
        if self.type.startswith("INT"):
            if self.primary:
                epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
                return (value.astimezone(tz.UTC) - epoch).total_seconds()
            return int(value)
        if self.type == "FLOAT":
            return float(value)
        return value

    def decode(self, value: Any) -> Any:
        if pd.isna(value):
            return None
        if self.type.startswith("TIMESTAMP"):
            return pd.Timestamp(value, tz=tz.UTC)
        if self.type.startswith("INT"):
            if self.primary:
                return pd.Timestamp(value, unit="s", tz=tz.UTC)
            return int(value)
        if self.type == "FLOAT":
            return float(value)
        return value
