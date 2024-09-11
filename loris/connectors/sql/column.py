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

from sqlalchemy import Column as SAColumn, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base

import pandas as pd
from loris.core import ConfigurationException

Base = declarative_base()

class Column(Base):
    __tablename__ = 'columns'

    id = SAColumn(Integer, primary_key=True)
    name = SAColumn(String)
    column_type = SAColumn(String)
    length = SAColumn(Integer, nullable=True)
    default = SAColumn(String, nullable=True)
    nullable = SAColumn(Boolean, default=True)

    DEFAULT_NAME: str = "data"
    DEFAULT_TYPE: str = "FLOAT"

    @classmethod
    def from_defaults(cls) -> Column:
        return cls(name=cls.DEFAULT_NAME, column_type=cls.DEFAULT_TYPE)

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        name: str,
        column_type: AnyStr | Type | int,
        length: Optional[int] = None,
        default: Optional[Any] = None,
        nullable: bool = True,
    ):
        self.name = name
        self.column_type = self._parse_type(column_type, length)
        self.length = length
        self.default = default
        self.is_nullable = nullable

        self.flags = []
        if not nullable:
            self.flags.append("NOT NULL")
        self._logger = logging.getLogger(__name__)

    def __repr__(self) -> str:
        column = f"name={self.name}, type={self.column_type}"
        if self.length is not None:
            column += f"({self.length})"
        if len(self.flags) > 0:
            column += f", flags={' '.join(self.flags)}"
        return f"MySqlColumn({column})"

    def __str__(self) -> str:
        column = f"`{self.name}` {self.column_type}"
        if self.length is not None:
            column += f"({self.length})"
        if self.default is not None:
            column += f" DEFAULT {self.default}"
        if len(self.flags) > 0:
            column += f" {' '.join(self.flags)}"
        return column

    # noinspection PyShadowingBuiltins, PyUnresolvedReferences
    @staticmethod
    def _parse_type(column_type: AnyStr | Type | int, type_length: Optional[int] = None) -> str:
        if isinstance(column_type, str):
            column_type = column_type.upper()
            if column_type == "INT":
                return "INTEGER"
            elif column_type == "FLOAT":
                return "FLOAT"
            elif column_type == "VARCHAR":
                return "VARCHAR"
            elif column_type == "TIMESTAMP":
                return "TIMESTAMP"
            else:
                raise ConfigurationException(f"Unknown SQLAlchemy data type: {column_type}")

        if isinstance(column_type, int):
            return "INTEGER"

        if column_type == str: # noqa: E721
            return "VARCHAR" if type_length is not None else "STRING"

        if column_type in [pd.Timestamp, dt.datetime]:
            return "TIMESTAMP"

        if column_type == int: # noqa: E721
            return "INTEGER"

        if column_type == float: # noqa: E721
            return "FLOAT"

        raise ConfigurationException(f"Unknown SQLAlchemy data type: {column_type}")

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
        column_type: AnyStr | Type | int,
        length: Optional[int] = None,
        default: Optional[Any] = None,
        nullable: bool = True,
    ) -> None:
        self.append(Column(name, column_type, length=length, default=default, nullable=nullable))

    @property
    def names(self) -> List[str]:
        return [c.name for c in self]

    # noinspection PyShadowingBuiltins
    def filter(self, filter_func: Callable[[C], bool]):
        return type(self)(*[column for column in self if filter_func(column)])

    def extract(self, values: pd.Series) -> List[Any]:
        return [values.get(c.name, default=None) for c in self]
