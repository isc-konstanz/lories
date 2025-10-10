# -*- coding: utf-8 -*-
"""
lori.data.channels.channels
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from typing import Any, Iterator, Tuple

import numpy as np
import pandas as pd
from lori._core._channel import Channel, ChannelState, _Channel  # noqa
from lori._core._channels import Channels as ChannelsType  # noqa
from lori._core._channels import _Channels  # noqa
from lori.core import Resources
from lori.data.validation import validate_index

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class Channels(_Channels, Resources[Channel]):
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

    # noinspection PyTypeChecker
    def apply(self, apply: Callable[[Channel], Channel], inplace: bool = False) -> ChannelsType:
        return super().apply(apply, inplace=inplace)

    def filter(self, *filters: Callable[[Channel], bool]) -> ChannelsType:
        return super().filter(*filters)

    # noinspection PyShadowingBuiltins, SpellCheckingInspection
    def groupby(self, by: Callable[[Channel], Any] | str) -> Iterator[Tuple[Any, ChannelsType]]:
        return super().groupby(by)

    def from_logger(self) -> ChannelsType:
        return type(self)([c.from_logger() for c in self if c.has_logger()])

    def to_frame(self, unique: bool = False, states: bool = False) -> pd.DataFrame:
        data = OrderedDict()
        columns = list(self.keys if not unique else self.ids)
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
                    timestamp_data = data[timestamp] = {c: np.nan for c in columns}
                else:
                    timestamp_data = data[timestamp]

                    if any(not pd.isna(timestamp_data[k]) for k in channel_values.keys()):
                        self._logger.warning(
                            f"Overriding value for duplicate index while merging channel '{channel.id}' into "
                            f"DataFrame for index: {channel_data.index}"
                        )
                timestamp_data.update(channel_values)

        if len(data) == 0:
            return pd.DataFrame(columns=columns)
        data = pd.DataFrame.from_records(
            data=list(data.values()),
            index=list(data.keys()),
        )
        data.dropna(axis="index", how="all", inplace=True)
        data = validate_index(data)
        data.index.name = _Channel.TIMESTAMP
        return data

    # noinspection PyProtectedMember
    def set_frame(self, data: pd.DataFrame) -> None:
        for converter, channels in self.groupby(lambda c: c.converter._converter):
            converted_data = converter.from_frame(data, channels)
            for channel in channels:
                channel_data = converted_data.loc[:, channel.id].dropna()
                if channel_data.empty or channel_data.isna().all():
                    channel.state = ChannelState.NOT_AVAILABLE
                    self._logger.debug(f"Missing value for channel: {channel.id}")
                    continue

                timestamp = channel_data.index[0]
                if len(channel_data.index) == 1:
                    channel_data = channel_data.values[0]
                channel.set(timestamp, channel_data)

    def set_state(self, state: ChannelState) -> None:
        def _set_state(channel: Channel) -> Channel:
            channel.state = state
            return channel

        self.apply(_set_state, inplace=True)
