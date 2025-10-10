# -*- coding: utf-8 -*-
"""
lori._data._manager
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import Any, List, Optional, Tuple, TypeAlias, TypeVar, overload

import pandas as pd
from lori._core._activator import Activator, _Activator
from lori._core._channel import Channel, _Channel
from lori._core._channels import Channels, ChannelsArgument
from lori._core._connector import Connector
from lori._core._context import _Context
from lori._core._entity import _Entity
from lori._core.typing import Timestamp

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class _DataContext(_Context[Channel]):
    TYPE: str = "data"

    @property
    @abstractmethod
    def channels(self) -> Channels: ...

    @abstractmethod
    def filter(self, *filters: Optional[Callable[[Channel], bool]]) -> Channels: ...

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
    ) -> None: ...

    @abstractmethod
    def to_frame(self, **kwargs) -> pd.DataFrame: ...


class _DataManager(_DataContext, _Activator, _Entity):
    # noinspection PyShadowingBuiltins
    @abstractmethod
    def activate(self, filter: Optional[Callable[[Activator], bool]] = None) -> None: ...

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def connect(
        self,
        filter: Optional[Callable[[Connector], bool]] = None,
        channels: Optional[ChannelsArgument] = None,
        timeout: Optional[float] = None,
    ) -> None: ...

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def reconnect(
        self,
        filter: Optional[Callable[[Connector], bool]] = None,
    ) -> None: ...

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def disconnect(
        self,
        filter: Optional[Callable[[Connector], bool]] = None,
    ) -> None: ...

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def deactivate(self, *_, filter: Optional[Callable[[Activator], bool]] = None) -> None: ...


DataContext = TypeVar("DataContext", bound=_DataContext[_Channel])
DataManager = TypeVar("DataManager", bound=_DataManager)
Data: TypeAlias = DataContext
