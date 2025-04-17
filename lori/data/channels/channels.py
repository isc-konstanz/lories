# -*- coding: utf-8 -*-
"""
lori.data.channels.channels.Channels
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable

import pandas as pd
from lori.core import Resources
from lori.data.channels import Channel, ChannelState
from lori.data.validation import validate_index

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class Channels(Resources[Channel]):
    SECTION: str = "channels"

    def __str__(self) -> str:
        return str(self.to_frame(unique=True, states=True))

    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        for channel in self:
            channel.register(function, how=how, unique=unique)

    def to_frame(self, unique: bool = False, states: bool = False) -> pd.DataFrame:
        data = OrderedDict()
        for channel in self:
            if pd.isna(channel.timestamp):
                continue
            channel_uid = channel.key if not unique else channel.id
            channel_data = channel.to_series(state=states)
            channel_data.name = channel_uid
            if channel_data.empty:
                continue
            for timestamp, channel_values in channel_data.to_frame().to_dict(orient="index").items():
                if timestamp not in data:
                    timestamp_data = data[timestamp] = {}
                else:
                    timestamp_data = data[timestamp]

                    if any(k in timestamp_data for k in channel_values.keys()):
                        self._logger.warning(
                            f"Overriding value for duplicate index while merging channel '{channel.id}' into "
                            f"DataFrame for index: {channel_data.index}"
                        )
                timestamp_data.update(channel_values)

        if len(data) == 0:
            return pd.DataFrame()
        data = pd.DataFrame.from_records(
            data=list(data.values()),
            index=list(data.keys()),
        )
        data.dropna(axis="index", how="all", inplace=True)
        data.dropna(axis="columns", how="all", inplace=True)
        data = validate_index(data)
        data.index.name = Channel.TIMESTAMP
        return data

    # noinspection PyProtectedMember
    def set_frame(self, data: pd.DataFrame) -> None:
        for converter, channels in self.groupby(lambda c: c.converter._converter):
            converter_data = converter.convert(data, channels)
            for channel in channels:
                channel_data = converter_data.loc[:, channel.id].dropna()
                if channel_data.empty:
                    channel.state = ChannelState.NOT_AVAILABLE
                    if channel.id in data.columns:
                        self._logger.debug(f"Unable to update None value for channel: {channel.id}")
                    else:
                        self._logger.warning(f"Unable to update not configured channel: {channel.id}")
                    continue

                timestamp = channel_data.index[0]
                if len(channel_data.index) == 1:
                    channel_data = channel_data.values[0]
                channel.set(timestamp, channel_data)
