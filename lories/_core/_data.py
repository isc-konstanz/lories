# -*- coding: utf-8 -*-
"""
lories._core._data
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import Any, List, Literal, Optional, Tuple, TypeAlias, TypeVar, overload

import pandas as pd
from lories._core._channel import Channel, _Channel
from lories._core._channels import Channels, ChannelsArgument
from lories._core._context import _Context
from lories._core.typing import Timestamp


class _DataContext(_Context[Channel]):
    TYPE: str = "data"

    @property
    @abstractmethod
    def channels(self) -> Channels: ...

    # noinspection PyShadowingBuiltins
    @overload
    def filter(self, filter: Callable[[Channel], bool]) -> Channels: ...

    # noinspection PyShadowingBuiltins
    @overload
    def filter(self, filter: Callable[[Channel], bool], *other: Callable[[Channel], bool]) -> Channels: ...

    @abstractmethod
    def filter(self, *filters: Callable[[Channel], bool]) -> Channels: ...

    # noinspection SpellCheckingInspection
    @abstractmethod
    def groupby(self, by: str) -> List[Tuple[Any, Channels]]: ...

    @abstractmethod
    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        channels: Optional[ChannelsArgument] = None,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        pass

    @overload
    def has_logged(
        self,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> bool: ...

    @overload
    def has_logged(
        self,
        channels: ChannelsArgument,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> bool: ...

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def has_logged(
        self,
        channels: Optional[ChannelsArgument] = None,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> bool: ...

    @overload
    def read_logged(
        self,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> pd.DataFrame: ...

    @overload
    def read_logged(
        self,
        channels: ChannelsArgument,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def read_logged(
        self,
        channels: Optional[ChannelsArgument] = None,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> pd.DataFrame: ...

    @overload
    def read(
        self,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> pd.DataFrame: ...

    @overload
    def read(
        self,
        channels: ChannelsArgument,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def read(
        self,
        channels: Optional[ChannelsArgument] = None,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> pd.DataFrame: ...

    @overload
    def write(
        self,
        data: pd.DataFrame,
        timeout: Optional[float] = None,
    ) -> pd.DataFrame: ...

    @overload
    def write(
        self,
        data: pd.DataFrame,
        channels: ChannelsArgument,
        timeout: Optional[float] = None,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def write(
        self,
        data: pd.DataFrame,
        channels: Optional[ChannelsArgument] = None,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> None: ...

    @abstractmethod
    def to_frame(self, **kwargs) -> pd.DataFrame: ...


DataContext = TypeVar("DataContext", bound=_DataContext[_Channel])
Data: TypeAlias = DataContext
