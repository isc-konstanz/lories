# -*- coding: utf-8 -*-
"""
lories._core._channels
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Iterable, TypeVar, Union

import pandas as pd
from lories._core._channel import ChannelState, _Channel
from lories._core._resources import _Resources


class _Channels(_Resources[_Channel]):
    TYPE: str = "channels"

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
