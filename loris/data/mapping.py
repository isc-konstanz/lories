# -*- coding: utf-8 -*-
"""
    loris.data.mapping
    ~~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from collections.abc import Mapping
from collections import OrderedDict
from typing import Iterator

import pandas as pd

from loris import Channel, Channels


class DataMapping(Mapping[str, Channel]):

    _channels: OrderedDict[str, Channel]

    def __init__(self, channels=(), *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._channels = OrderedDict(channels)

    def __getattr__(self, attr):
        channels = DataMapping.__getattribute__(self, '_channels')
        if attr in channels.keys():
            return channels[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no channel '{attr}'")

    def __getitem__(self, channel_id: str) -> Channel:
        return self._channels[channel_id]

    def __len__(self) -> int:
        return len(self._channels)

    def __iter__(self) -> Iterator[str, Channel]:
        return iter(self._channels)

    def values(self) -> Channels:
        return Channels(self._channels.values())

    # noinspection PyShadowingBuiltins
    def filter(self, filter: callable) -> DataMapping:
        return DataMapping({i: c for i, c in self._channels.items() if filter(c)})

    def to_frame(self) -> pd.DataFrame:
        return self.values().to_frame()
