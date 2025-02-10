# -*- coding: utf-8 -*-
"""
lori.converters.converter
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from typing import Any, Generic, List, Type, TypeVar, overload

import pandas as pd
from lori.core import Registrator, ResourceException
from lori.util import is_bool, is_float, is_int, to_bool, to_date, to_float, to_int

T = TypeVar("T", bound=Any)


class Converter(Registrator, Generic[T]):
    SECTION: str = "converter"
    INCLUDES: List[str] = []

    @property
    @abstractmethod
    def dtype(self) -> Type[T]: ...

    @abstractmethod
    def is_dtype(self, value: str | T) -> bool: ...

    @overload
    def convert(self, value: str | T) -> T: ...

    @overload
    def convert(self, value: pd.Series) -> pd.Series: ...

    @abstractmethod
    def convert(self, value: str | T | pd.Series) -> T | pd.Series: ...

    @overload
    def revert(self, value: str | T) -> T: ...

    @overload
    def revert(self, value: pd.Series) -> pd.Series: ...

    @abstractmethod
    def revert(self, value: T | pd.Series) -> str | pd.Series: ...


class ConversionException(ResourceException, TypeError):
    """
    Raise if a conversion failed
    """


class GenericConverter(Converter, Generic[T]):
    def is_dtype(self, value: str | T) -> bool:
        if isinstance(value, (str, self.dtype)):
            return True
        return False

    def convert(self, value: str | float | pd.Series) -> T | pd.Series:
        try:
            if issubclass(type(value), pd.Series) and all(value.apply(self.is_dtype)):
                return value.apply(self._convert)  # .astype(self.dtype)
            elif self.is_dtype(value):
                return self._convert(value)
        except TypeError:
            pass
        raise ConversionException(f"Expected str or {self.dtype}, not: {type(value)}")

    @abstractmethod
    def _convert(self, value: str | T) -> T:
        pass

    def revert(self, value: T | pd.Series) -> str | pd.Series:
        if issubclass(type(value), pd.Series):
            return value.apply(lambda v: str(v)).astype(str)
        return str(value)


class DatetimeConverter(GenericConverter[dt.datetime]):
    dtype: Type[dt.datetime] = dt.datetime

    def _convert(self, value: str | pd.Timestamp | dt.datetime) -> pd.Timestamp | dt.datetime:
        return to_date(value)


class TimestampConverter(DatetimeConverter):
    dtype: Type[pd.Timestamp] = pd.Timestamp


class StringConverter(GenericConverter[str]):
    dtype: Type[str] = str

    def _convert(self, value: Any) -> str:
        return str(value)


class FloatConverter(GenericConverter[float]):
    dtype: Type[float] = float

    def is_dtype(self, value: str | float) -> bool:
        return is_float(value)

    def _convert(self, value: str | float) -> float:
        return to_float(value)


class IntConverter(GenericConverter[int]):
    dtype: Type[int] = int

    def is_dtype(self, value: str | int) -> bool:
        return is_int(value)

    def _convert(self, value: str | int) -> int:
        return to_int(value)


class BoolConverter(GenericConverter[bool]):
    dtype: Type[bool] = bool

    def is_dtype(self, value: str | bool) -> bool:
        return is_bool(value)

    def _convert(self, value: str | bool) -> bool:
        return to_bool(value)
