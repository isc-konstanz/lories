# -*- coding: utf-8 -*-
"""
lories._core._converter
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Collection, Generic, Optional, Type, TypeAlias, TypeVar, overload

import pandas as pd
from lories._core._channel import Channel
from lories._core._channels import Channels
from lories._core._registrator import _Registrator, _RegistratorContext
from lories._core.typing import Timestamp

T = TypeVar("T", bound=Any)


class _Converter(_Registrator, Generic[T]):
    INCLUDES: Collection[str] = ()
    TYPE: str = "converter"

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

    @abstractmethod
    def to_json(self, data: T | pd.Series | pd.DataFrame) -> str: ...

    @abstractmethod
    def to_series(
        self,
        value: T,
        timestamp: Optional[Timestamp] = None,
        name: Optional[str] = None,
    ) -> pd.Series: ...

    @abstractmethod
    def from_frame(
        self,
        data: pd.DataFrame,
        channels: Channels,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def from_series(
        self,
        data: pd.Series,
        channel: Channel,
    ) -> pd.Series: ...

    @abstractmethod
    def convert(self, value: Any, **kwargs) -> Optional[T]: ...


Converter = TypeVar("Converter", bound=_Converter)


# noinspection PyAbstractClass
class _ConverterContext(_RegistratorContext[Converter]):
    TYPE: str = "converters"


ConverterContext = TypeVar(
    name="ConverterContext",
    bound=_ConverterContext,
)
Converters: TypeAlias = ConverterContext
