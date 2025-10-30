# -*- coding: utf-8 -*-
"""
lories.data.converters.converter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any, Generic, Optional, Type, TypeVar

import pandas as pd
import pytz as tz
from lories._core import _Channel, _Channels, _Converter  # noqa
from lories.core import Registrator
from lories.data.converters.errors import ConversionError
from lories.data.validation import validate_index
from lories.util import is_bool, is_float, is_int, to_bool, to_date, to_float, to_int

T = TypeVar("T", bound=Any)


# noinspection PyAbstractClass
class Converter(_Converter, Registrator, Generic[T]):
    def to_str(self, value: T | pd.Series) -> str:
        return self.to_json(value)

    # noinspection PyMethodMayBeStatic
    def to_json(self, data: T | pd.Series | pd.DataFrame) -> str:
        if issubclass(type(data), (pd.Series, pd.DataFrame)):
            if issubclass(type(data), pd.Series):
                data = data.to_frame()
            columns = data.columns
            data[_Channel.TIMESTAMP] = data.index
            return data[[_Channel.TIMESTAMP, *columns]].to_json(orient="records")
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

        series = validate_index(series)
        series.index.name = _Channel.TIMESTAMP
        if series.index.tzinfo is None:
            self._logger.warning(
                f"UTC will be presumed for timestamps, as DatetimeIndex are expected to be tz-aware: {series}"
            )
            series.index = series.index.tz_localize(tz.UTC)
        return series

    # noinspection PyProtectedMember
    def from_frame(self, data: pd.DataFrame, channels: _Channels) -> pd.DataFrame:
        converted_data = []
        for channel in channels:
            channel_data = data[channel.id].dropna() if channel.id in data.columns else None
            if channel_data is None or channel_data.empty:
                converted_data.append(pd.Series(name=channel.id))
                continue
            elif not channel_data.apply(self.is_dtype).all():
                converted_data.append(pd.Series(name=channel.id))
                self._logger.warning(f"Unable to convert values for channel '{channel.id}': {channel_data.values}")
                continue
            try:
                converted_data.append(self.from_series(channel_data, channel))
            except TypeError:
                raise ConversionError(f"Expected str or {self.dtype}, not: {type(data)}")
        if len(converted_data) == 0:
            return pd.DataFrame(columns=[c.id for c in channels])
        return pd.concat(converted_data, axis="columns")

    # noinspection PyProtectedMember, PyUnresolvedReferences
    def from_series(self, data: pd.Series, channel: _Channel) -> pd.Series:
        try:
            converter_args = channel.converter._get_configs()
            converted_data = data.apply(self.convert, **converter_args)
            return converted_data.apply(self.to_dtype, **converter_args)
        except TypeError:
            raise ConversionError(f"Expected str or {self.dtype}, not: {type(data)}")

    # noinspection PyMethodMayBeStatic, PyUnusedLocal
    def convert(self, value: Any, **kwargs) -> Optional[T]:
        return value


# noinspection PyAbstractClass, PyMethodMayBeStatic
class _NumberConverter(Converter[T]):
    def scale(self, value: T, factor: Optional[T], invert: bool = False, **kwargs) -> T:
        if value is not None and factor is not None:
            if invert:
                value /= factor
            else:
                value *= factor
        return self.to_dtype(value, **kwargs)

    # noinspection PyProtectedMember, PyUnresolvedReferences
    def from_series(self, data: pd.Series, channel: _Channel) -> pd.Series:
        try:
            converter_args = channel.converter._get_configs()
            converter_args["factor"] = to_float(channel.get("scale", default=None))
            converted_data = data.apply(self.convert, **converter_args)
            return converted_data.apply(self.scale, **converter_args)
        except TypeError:
            raise ConversionError(f"Expected str or {self.dtype}, not: {type(data)}")


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
class FloatConverter(_NumberConverter[float]):
    dtype: Type[float] = float

    def is_dtype(self, value: str | float) -> bool:
        return is_float(value)

    def to_dtype(self, value: str | float, decimals: Optional[int] = None) -> Optional[float]:
        value = to_float(value)
        if decimals is not None:
            value = round(value, decimals)
        return value


# noinspection PyMethodMayBeStatic
class IntConverter(_NumberConverter[int]):
    dtype: Type[int] = int

    def is_dtype(self, value: str | int) -> bool:
        return is_int(value)

    def to_dtype(self, value: str | int, **_) -> Optional[int]:
        return to_int(value)


# noinspection PyMethodMayBeStatic
class BoolConverter(Converter[bool]):
    dtype: Type[bool] = bool

    def is_dtype(self, value: str | bool) -> bool:
        return is_bool(value) or is_int(value)

    def to_dtype(self, value: str | bool, **_) -> Optional[bool]:
        return to_bool(value)


# noinspection PyMethodMayBeStatic
class BytesConverter(Converter[bytes]):
    dtype: Type[bytes] = bytes

    def is_dtype(self, value: str | bytes) -> bool:
        return isinstance(value, bytes)

    def to_dtype(self, value: str | bytes, **_) -> Optional[bytes]:
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode()
        return None
