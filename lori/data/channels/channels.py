# -*- coding: utf-8 -*-
"""
lori.data.channels.channels.Channels
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from typing import Any, Literal, Sequence

import pandas as pd
import pytz as tz
from lori.core import Resources
from lori.data.channels import Channel, ChannelState


class Channels(Resources[Channel]):
    def __str__(self) -> str:
        return str(self.to_frame(unique=True, states=True))

    @property
    def ids(self) -> Sequence[str]:
        return [c.id for c in self]

    @property
    def keys(self) -> Sequence[str]:
        return [c.key for c in self]

    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        for channel in self:
            channel.register(function, how=how, unique=unique)

    def to_frame(self, unique: bool = False, states: bool = False) -> pd.DataFrame:
        columns = []
        data = OrderedDict[pd.Timestamp, Any]()

        # noinspection PyTypeChecker
        def append(column: str, series: pd.Series) -> None:
            for index, value in series.items():
                if index in data:
                    row = data[index]
                else:
                    row = {}
                    data[index] = row
                if column in row:
                    self._logger.warning(
                        f"Overriding value for duplicate index while merging channel '{column}' into "
                        f"DataFrame for index: {index}"
                    )
                row[column] = value
            if column not in columns:
                columns.append(column)

        for channel in self:
            if pd.isna(channel.timestamp):
                continue
            channel_uid = channel.key if not unique else channel.id
            channel_data = channel.to_series(state=states)
            channel_data.name = channel_uid

            if channel_data.index.tzinfo is None:
                self._logger.warning(
                    f"UTC will be presumed for channel '{channel.id}' timestamps, "
                    f"as tz-naive with tz-aware DatetimeIndex cannot be joined: {channel_data}"
                )
                channel_data.index = channel_data.index.tz_localize(tz.UTC)
            append(channel_uid, channel_data)

        if len(data) > 0:
            return pd.DataFrame.from_records(list(data.values()), index=list(data.keys()), columns=columns)
        return pd.DataFrame(columns=columns)

    def set_frame(self, data: pd.DataFrame) -> None:
        for channel in self:
            if channel.id not in data.columns or all(pd.isna(data.loc[:, channel.id])):
                channel.state = ChannelState.NOT_AVAILABLE
                self._logger.warning(f"Unable to update nonexisting channel: {channel.id}")
                continue

            channel_data = data.loc[:, channel.id].dropna()
            channel_data.name = channel.key
            if len(channel_data.index) > 1:
                timestamp = channel_data.index[0]
                channel.set(timestamp, channel_data)

            elif len(channel_data.index) > 0:
                timestamp = channel_data.index[-1]
                channel.set(timestamp, channel_data[timestamp])
