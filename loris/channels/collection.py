# -*- coding: utf-8 -*-
"""
    loris.core.channel.collection
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from collections.abc import Collection
from typing import Tuple, List, Iterator, Any

import numpy as np
import pandas as pd

from loris.channels import Channel


class Channels(Collection[Channel]):

    _channels: List[Channel]

    def __init__(self, channels=()) -> None:
        self._channels = list(channels)

    def __repr__(self) -> str:
        return str(self.to_frame())

    def __contains__(self, __x: object) -> bool:
        return __x in self._channels

    def __iter__(self) -> Iterator[Channel]:
        return iter(self._channels)

    def __len__(self) -> int:
        return len(self._channels)

    def _add(self, channel: Channel):
        self._channels.append(channel)

    def apply(self, apply: callable) -> None:
        for channel in self._channels:
            apply(channel)

    # noinspection PyShadowingBuiltins
    def filter(self, filter: callable) -> Channels:
        return Channels([channel for channel in self._channels if filter(channel)])

    # noinspection SpellCheckingInspection
    def groupby(self, by: str) -> List[Tuple[Any, Channels]]:
        groups = []
        for group_by in np.unique([getattr(c, by) for c in self._channels]):
            groups.append((group_by, self.filter(lambda c: getattr(c, by) == group_by)))
        return groups

    # noinspection PyProtectedMember
    def to_frame(self, unique: bool = False) -> pd.DataFrame:
        columns = []
        data = []
        for channel in self._channels:
            channel_id = channel.id if not unique else channel._uuid
            if not isinstance(channel.value, (pd.Series, pd.DataFrame)):
                channel_data = pd.Series(index=[channel.time], data=[channel.value], name=channel_id)
            else:
                channel_data = channel.value
                channel_data.name = channel_id
            columns.append(channel_id)
            data.append(channel_data)
        if len(data) > 0:
            return pd.concat(data, axis='columns', keys=columns)\
                .dropna(axis='index', how='all')\
                .dropna(axis='columns', how='all')
        else:
            return pd.DataFrame(columns=columns)
