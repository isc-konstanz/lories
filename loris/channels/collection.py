# -*- coding: utf-8 -*-
"""
loris.core.channel.collection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from collections.abc import Collection
from typing import Any, Iterator, List, Tuple

import numpy as np
import pandas as pd
import pytz as tz
from loris.channels import Channel


class Channels(Collection[Channel]):
    _channels: List[Channel]

    def __init__(self, channels=()) -> None:
        self._logger = logging.getLogger(__name__)
        self._channels = [*channels]

    def __repr__(self) -> str:
        # return str(self.to_frame(states=True))
        return str([c.id for c in self])

    def __contains__(self, __x: object) -> bool:
        return __x in self._channels

    def __iter__(self) -> Iterator[Channel]:
        return iter(self._channels)

    def __len__(self) -> int:
        return len(self._channels)

    def _add(self, channel: Channel):
        self._channels.append(channel)

    def copy(self) -> Channels:
        return Channels([channel.copy() for channel in self._channels])

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

    def to_frame(self, unique: bool = False, states: bool = False) -> pd.DataFrame:
        columns = []
        data = []
        for channel in self._channels:
            channel_id = channel.id if not unique else channel.uuid
            channel_data = channel.to_series(state=states)
            channel_data.name = channel_id

            if channel_data.index.tzinfo is None:
                self._logger.warning(
                    f'UTC will be presumed for channel "{channel.uuid}" timestamps, '
                    f"as tz-naive with tz-aware DatetimeIndex cannot be joined: {channel_data}"
                )
                channel_data.index = channel_data.index.tz_localize(tz.UTC)

            columns.append(channel_id)
            data.append(channel_data)
        if len(data) > 0:
            return pd.concat(data, axis="columns", keys=columns)  # .dropna(axis='columns', how='all')
        return pd.DataFrame(columns=columns)
