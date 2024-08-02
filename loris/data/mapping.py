# -*- coding: utf-8 -*-
"""
loris.data.mapping
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from typing import Any, Iterator, List, Tuple

import numpy as np
import pandas as pd
from loris.data.channels import Channel, Channels


class DataMapping(Mapping[str, Channel]):
    _channels: OrderedDict[str, Channel]

    def __init__(self, channels=(), *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._channels = OrderedDict(channels)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({[c.id for c in self._channels.values()]})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join([f"{i} = {repr(c)}" for i, c in self._channels.items()])

    def __getattr__(self, attr):
        channels = DataMapping.__getattribute__(self, "_channels")
        if attr in channels.keys():
            return channels[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no channel '{attr}'")

    def __getitem__(self, uid: str) -> Channel:
        return self._get(uid)

    def __contains__(self, channel: str | Channel) -> bool:
        if isinstance(channel, str):
            return channel in self._channels.keys()
        if isinstance(channel, Channel):
            return channel in self._channels.values()
        return False

    def __len__(self) -> int:
        return len(self._channels)

    def __iter__(self) -> Iterator[str]:
        return iter(self._channels)

    def _get(self, uid: str) -> Channel:
        return self._channels[uid]

    def values(self) -> Channels:
        return Channels(self._channels.values())

    # noinspection PyShadowingBuiltins
    def filter(self, filter: callable) -> Channels:
        return Channels([c for c in self._channels.values() if filter(c)])

    # noinspection SpellCheckingInspection
    def groupby(self, by: str) -> List[Tuple[Any, Channels]]:
        groups = []
        for group_by in np.unique([getattr(c, by) for c in self._channels.values()]):
            groups.append((group_by, self.filter(lambda c: getattr(c, by) == group_by)))
        return groups

    def to_frame(self, **kwargs) -> pd.DataFrame:
        return self.values().to_frame(**kwargs)
