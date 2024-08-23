# -*- coding: utf-8 -*-
"""
loris.connectors.sql.column
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import logging
from collections import UserList
from typing import Any, AnyStr, Callable, Generic, List, Optional, Type, TypeVar

from mysql.connector import FieldType

import pandas as pd
from loris.core import ConfigurationException


class Column:
    DEFAULT_NAME: str = "data"
    DEFAULT_TYPE: str = "FLOAT"

    name: str
    type: str
    length: Optional[int]

    default: Optional[Any]
    flags: List[str]

    @classmethod
    def from_defaults(cls) -> Column:
        return cls(cls.DEFAULT_NAME, cls.DEFAULT_TYPE)

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        name: str,
        type: AnyStr | Type | int,
        length: Optional[int] = None,
        default: Optional[Any] = None,
        nullable: bool = True,
    ):
        self.name = name
        self.type = self._parse_type(type, length)
        self.length = length
        self.default = default

        self.flags = []
        if not nullable:
            self.flags.append("NOT NULL")
        self._logger = logging.getLogger(__name__)

    def __repr__(self) -> str:
        column = f"name={self.name}, type={self.type}"
        if self.length is not None:
            column += f"({self.length})"
        if len(self.flags) > 0:
            column += f", flags={' '.join(self.flags)}"
        return f"MySqlColumn({column})"

    def __str__(self) -> str:
        column = f"`{self.name}` {self.type}"
        if self.length is not None:
            column += f"({self.length})"
        if self.default is not None:
            column += f" DEFAULT {self.default}"
        if len(self.flags) > 0:
            column += f" {' '.join(self.flags)}"
        return column

    @property
    def nullable(self) -> bool:
        return "NOT NULL" not in self.flags

    # noinspection PyShadowingBuiltins, PyUnresolvedReferences
    @staticmethod
    def _parse_type(type: AnyStr | Type | int, type_length: Optional[int] = None) -> str:
        if isinstance(type, str):
            type = type.upper()
            if type == "INT":
                # Plain "INT" is not in the FieldType list. Skip this for the following check
                # TODO: Validate necessity for this
                pass
            elif type not in [d[1] for d in FieldType.desc.values()]:
                raise ConfigurationException(f"Unknown MySQL data type: {type}")
            return type

        if isinstance(type, int):
            type_info = FieldType.get_info(type)
            if type_info is None:
                raise ConfigurationException(f"Unknown MySQL data type code: {type}")

            return type_info[1]

        if type == str:  # noqa: E721
            return "VARCHAR" if type_length is not None else "STRING"

        if type in [pd.Timestamp, dt.datetime]:
            return "TIMESTAMP"

        if type == int:  # noqa: E721
            return "INT"

        if type == float:  # noqa: E721
            return "FLOAT"

        raise ConfigurationException(f"Unknown MySQL data type: {type}")


C = TypeVar("C", bound=Column)


class Columns(UserList[C], Generic[C]):
    # noinspection PyTypeChecker
    @classmethod
    def from_defaults(cls) -> C:
        return cls(C.from_defaults())

    def __init__(self, *columns: C) -> None:
        super().__init__(columns)

    def __str__(self) -> str:
        return ", ".join(f"`{c.name}`" for c in self)

    # noinspection PyShadowingBuiltins
    def add(
        self,
        name: str,
        type: AnyStr | Type | int,
        length: Optional[int] = None,
        default: Optional[Any] = None,
        nullable: bool = True,
    ) -> None:
        self.append(Column(name, type, length=length, default=default, nullable=nullable))

    @property
    def names(self) -> List[str]:
        return [c.name for c in self]

    # noinspection PyShadowingBuiltins
    def filter(self, filter: Callable[[C], bool]):
        return type(self)(*[column for column in self if filter(column)])

    def extract(self, values: pd.Series) -> List[Any]:
        return [values.get(c.name, default=None) for c in self]
