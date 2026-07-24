# -*- coding: utf-8 -*-
"""
lories._core._listener
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import Collection, Optional, TypeAlias, TypeVar

import pandas as pd
from lories._core._channel import Channel
from lories._core._channels import Channels
from lories._core._context import _Context
from lories._core._entity import _Entity

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class _Listener(_Entity):
    @property
    @abstractmethod
    def timestamp(self) -> pd.Timestamp | pd.NaT: ...

    @property
    @abstractmethod
    def runtime(self) -> Optional[float]: ...

    @abstractmethod
    def run(self) -> None: ...

    @abstractmethod
    def locked(self) -> bool: ...

    @abstractmethod
    def has_update(self) -> bool: ...


Listener = TypeVar("Listener", bound=_Listener)


# noinspection PyAbstractClass
class _ListenerContext(_Context[Listener]):
    TYPE: str = "listeners"

    @abstractmethod
    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        channels: Channels,
        how: Literal["any", "all"] = "any",
        unique: bool = False,
        interval: Optional[str | pd.Timedelta] = None,
    ) -> None: ...

    @abstractmethod
    def notify(self, *channels: Channel) -> Collection[Listener]: ...


ListenerContext = TypeVar(
    name="ListenerContext",
    bound=_ListenerContext,
)
Listeners: TypeAlias = _ListenerContext
