# -*- coding: utf-8 -*-
"""
lori._data._channels
~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import Iterable, TypeVar, Union

import pandas as pd
from lori._core._channel import ChannelState, _Channel
from lori._core._resources import _Resources

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class _Channels(_Resources[_Channel]):
    TYPE: str = "channels"

    @abstractmethod
    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None: ...

    @abstractmethod
    def from_logger(self) -> Channels: ...

    @abstractmethod
    def to_frame(self, unique: bool = False, states: bool = False) -> pd.DataFrame: ...

    @abstractmethod
    # noinspection PyProtectedMember
    def set_frame(self, data: pd.DataFrame) -> None: ...

    @abstractmethod
    def set_state(self, state: ChannelState) -> None: ...


Channels = TypeVar("Channels", bound=_Channels)

ChannelsArgument = TypeVar(
    "ChannelsArgument",
    bound=Union[_Channel, _Channels, Iterable[_Channel], Iterable[str], str],
)
