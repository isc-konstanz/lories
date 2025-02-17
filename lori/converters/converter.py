# -*- coding: utf-8 -*-
"""
lori.converters.converter
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import json
from abc import abstractmethod
from typing import Any, Collection, Generic, List, Optional, Type, TypeVar, overload

import pandas as pd
import pytz as tz
from lori.core import Registrator, ResourceException
from lori.data.channels import Channel, Channels
from lori.util import is_bool, is_float, is_int, to_bool, to_date, to_float, to_int

T = TypeVar("T", bound=Any)


class Converter(Registrator, Generic[T]):
    SECTION: str = "converter"
    INCLUDES: List[str] = []

    @property
    @abstractmethod
    def dtype(self) -> Type[T]: ...

    @abstractmethod
    def is_dtype(self, value: Any) -> bool: ...

    @abstractmethod
    def to_dtype(self, value: Any, **kwargs) -> Optional[T]: ...

    @overload
    def to_str(self, value: T) -> str: ...

    @overload
    def to_str(self, value: pd.Series) -> str: ...

    def to_str(self, value: T | pd.Series) -> str:
        return self.to_json(value)

    @overload
    def to_json(self, value: T) -> str: ...

    @overload
    def to_json(self, value: pd.Series) -> str: ...

    @overload
    def to_json(self, value: pd.DataFrame) -> str: ...

    # noinspection PyMethodMayBeStatic
    def to_json(self, data: T | pd.Series | pd.DataFrame) -> str:
        if issubclass(type(data), (pd.Series, pd.DataFrame)):
            if issubclass(type(data), pd.Series):
                data = data.to_frame()
            columns = data.columns
            data[Channel.TIMESTAMP] = data.index
            return data[[Channel.TIMESTAMP, *columns]].to_json(orient="records")
        return json.dumps(data)

    # noinspection PyMethodMayBeStatic
    def to_series(self, value: T, timestamp: Optional[pd.Timestamp] = None, name: Optional[str] = None) -> pd.Series:
        if timestamp is None:
            timestamp = pd.Timestamp.now(tz=tz.UTC)
        if isinstance(value, pd.Series):
            series = value
            series.name = name
        else:
            series = pd.Series(index=[timestamp], data=[value], name=name)

        if not series.index.is_unique or not isinstance(series.index, pd.DatetimeIndex):
            raise ResourceException(f"Invalid index for series, DatetimeIndex is expected: {series}")

        series.index.name = Channel.TIMESTAMP
        if series.index.tzinfo is None:
            self._logger.warning(
                f"UTC will be presumed for timestamps, as DatetimeIndex are expected to be tz-aware: {series}"
            )
            series.index = series.index.tz_localize(tz.UTC)
        return series

    # noinspection PyProtectedMember
    def convert(self, data: pd.DataFrame, channels: Channels) -> pd.DataFrame:
        converted_data = []
        for channel in channels:
            channel_data = data.loc[:, channel.id].dropna() if channel.id in data.columns else None
            if channel_data is None or channel_data.empty or not all(channel_data.apply(self.is_dtype)):
                converted_data.append(pd.Series(name=channel.id))
                continue
            try:
                converter_args = channel.converter._get_configs()
                converted_data.append(channel_data.apply(self.to_dtype, **converter_args))
            except TypeError:
                raise ConversionException(f"Expected str or {self.dtype}, not: {type(data)}")
        if len(converted_data) == 0:
            return pd.DataFrame(columns=[c.id for c in channels])
        return pd.concat(converted_data, axis="columns")


class ConversionException(ResourceException, TypeError):
    """
    Raise if a conversion failed
    """


# noinspection PyMethodMayBeStatic
class DatetimeConverter(Converter[dt.datetime]):
    dtype: Type[dt.datetime] = dt.datetime

    def is_dtype(self, value: str | dt.datetime) -> bool:
        return isinstance(value, (str, self.dtype))

    def to_dtype(self, value: str | dt.datetime, **_) -> Optional[dt.datetime]:
        return to_date(value)


# noinspection PyMethodMayBeStatic
class TimestampConverter(DatetimeConverter):
    dtype: Type[pd.Timestamp] = pd.Timestamp

    def is_dtype(self, value: str | pd.Timestamp) -> bool:
        return isinstance(value, (str, self.dtype))

    def to_dtype(self, value: str | pd.Timestamp, **_) -> Optional[pd.Timestamp]:
        return to_date(value)


# noinspection PyMethodMayBeStatic
class StringConverter(Converter[str]):
    dtype: Type[str] = str

    def is_dtype(self, value: str) -> bool:
        return isinstance(value, str)

    def to_dtype(self, value: Any, **_) -> Optional[str]:
        return str(value)


# noinspection PyMethodMayBeStatic
class FloatConverter(Converter[float]):
    dtype: Type[float] = float

    def is_dtype(self, value: str | float) -> bool:
        return is_float(value)

    def to_dtype(self, value: str | float, decimals: Optional[int] = None) -> Optional[float]:
        value = to_float(value)
        if decimals is not None:
            value = round(value, decimals)
        return value


# noinspection PyMethodMayBeStatic
class IntConverter(Converter[int]):
    dtype: Type[int] = int

    def is_dtype(self, value: str | int) -> bool:
        return is_int(value)

    def to_dtype(self, value: str | int, **_) -> Optional[int]:
        return to_int(value)


# noinspection PyMethodMayBeStatic
class BoolConverter(Converter[bool]):
    dtype: Type[bool] = bool

    def is_dtype(self, value: str | bool) -> bool:
        return is_bool(value)

    def to_dtype(self, value: str | bool, **_) -> Optional[bool]:
        return to_bool(value)
