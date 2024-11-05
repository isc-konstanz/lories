# -*- coding: utf-8 -*-
"""
lori.connectors.sql.column
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import logging
from collections import UserList
from typing import Any, AnyStr, Callable, Dict, Generic, List, Optional, Tuple, Type, TypeVar

from sqlalchemy import Boolean, Integer, String
from sqlalchemy import Column as SAColumn
from sqlalchemy.ext.declarative import declarative_base

import pandas as pd
from lori.core import ConfigurationException

Base = declarative_base()


class FieldType:
    """SQLAlchemy Field Types."""

    prefix: str = "FIELD_TYPE_"
    DECIMAL: str = "DECIMAL"
    TINY: str = "TINY"
    SHORT: str = "SHORT"
    LONG: str = "LONG"
    FLOAT: str = "FLOAT"
    DOUBLE: str = "DOUBLE"
    NULL: str = "NULL"
    TIMESTAMP: str = "TIMESTAMP"
    LONGLONG: str = "LONGLONG"
    INT24: str = "INT24"
    DATE: str = "DATE"
    TIME: str = "TIME"
    DATETIME: str = "DATETIME"
    YEAR: str = "YEAR"
    NEWDATE: str = "NEWDATE"
    VARCHAR: str = "VARCHAR"
    BIT: str = "BIT"
    VECTOR: str = "VECTOR"
    JSON: str = "JSON"
    NEWDECIMAL: str = "NEWDECIMAL"
    ENUM: str = "ENUM"
    SET: str = "SET"
    TINY_BLOB: str = "TINY_BLOB"
    MEDIUM_BLOB: str = "MEDIUM_BLOB"
    LONG_BLOB: str = "LONG_BLOB"
    BLOB: str = "BLOB"
    VAR_STRING: str = "VAR_STRING"
    STRING: str = "STRING"
    GEOMETRY: str = "GEOMETRY"

    desc: Dict[str, Tuple[str, str]] = {
        "DECIMAL": (DECIMAL, "DECIMAL"),
        "TINY": (TINY, "TINY"),
        "SHORT": (SHORT, "SHORT"),
        "LONG": (LONG, "LONG"),
        "FLOAT": (FLOAT, "FLOAT"),
        "DOUBLE": (DOUBLE, "DOUBLE"),
        "NULL": (NULL, "NULL"),
        "TIMESTAMP": (TIMESTAMP, "TIMESTAMP"),
        "LONGLONG": (LONGLONG, "LONGLONG"),
        "INT24": (INT24, "INT24"),
        "DATE": (DATE, "DATE"),
        "TIME": (TIME, "TIME"),
        "DATETIME": (DATETIME, "DATETIME"),
        "YEAR": (YEAR, "YEAR"),
        "NEWDATE": (NEWDATE, "NEWDATE"),
        "VARCHAR": (VARCHAR, "VARCHAR"),
        "BIT": (BIT, "BIT"),
        "VECTOR": (VECTOR, "VECTOR"),
        "JSON": (JSON, "JSON"),
        "NEWDECIMAL": (NEWDECIMAL, "NEWDECIMAL"),
        "ENUM": (ENUM, "ENUM"),
        "SET": (SET, "SET"),
        "TINY_BLOB": (TINY_BLOB, "TINY_BLOB"),
        "MEDIUM_BLOB": (MEDIUM_BLOB, "MEDIUM_BLOB"),
        "LONG_BLOB": (LONG_BLOB, "LONG_BLOB"),
        "BLOB": (BLOB, "BLOB"),
        "VAR_STRING": (VAR_STRING, "VAR_STRING"),
        "STRING": (STRING, "STRING"),
        "GEOMETRY": (GEOMETRY, "GEOMETRY"),
    }

    @staticmethod
    def get_info(_type: Any) -> Optional[Tuple[str, str]]:
        for key, value in FieldType.desc.items():
            if value[0] == _type:
                return value
        return None


class Column(Base):
    __tablename__ = "columns"

    id = SAColumn(Integer, primary_key=True)
    name = SAColumn(String)
    type = SAColumn(String)
    length = SAColumn(Integer, nullable=True)
    default = SAColumn(String, nullable=True)
    nullable = SAColumn(Boolean, default=True)

    DEFAULT_NAME: str = "data"
    DEFAULT_TYPE: str = "FLOAT"

    @classmethod
    def from_defaults(cls) -> Column:
        return cls(name=cls.DEFAULT_NAME, type=cls.DEFAULT_TYPE)

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
        self.is_nullable = nullable

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
        return f"Column({column})"

    def __str__(self) -> str:
        column = f"`{self.name}` {self.type}"
        if self.length is not None:
            column += f"({self.length})"
        if self.default is not None:
            column += f" DEFAULT {self.default}"
        if len(self.flags) > 0:
            column += f" {' '.join(self.flags)}"
        return column

    # noinspection PyShadowingBuiltins, PyUnresolvedReferences
    @staticmethod
    def _parse_type(type: AnyStr | Type | int, type_length: Optional[int] = None) -> str:
        if isinstance(type, str):
            type = type.upper()
            if type == "INT":
                # Plain "INT" is not in the SQLAlchemy type list. Skip this for the following check
                # TODO: Validate necessity for this
                pass
            elif type not in [d[1] for d in FieldType.desc.values()]:
                raise ConfigurationException(f"Unknown SQLAlchemy data type: {type}")
            return type

        if isinstance(type, int):
            type_info = FieldType.get_info(type)
            if type_info is None:
                raise ConfigurationException(f"SQLAlchemy does not use integer type codes: {type}")

            return type_info[1]

        if type == str:  # noqa: E721
            return "VARCHAR" if type_length is not None else "STRING"

        if type in [pd.Timestamp, dt.datetime]:
            return "TIMESTAMP"

        if type == int:  # noqa: E721
            return "INTEGER"

        if type == float:  # noqa: E721
            return "FLOAT"

        raise ConfigurationException(f"Unknown SQLAlchemy data type: {type}")


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
    def filter(self, filter_func: Callable[[C], bool]):
        return type(self)(*[column for column in self if filter_func(column)])

    def extract(self, values: pd.Series) -> List[Any]:
        return [values.get(c.name, default=None) for c in self]
