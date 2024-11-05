# -*- coding: utf-8 -*-
"""
lori.data.channels.channels.Channels
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import pandas as pd
import pytz as tz
from lori.core import Resources
from lori.data.channels import Channel


class Channels(Resources[Channel]):
    def __str__(self) -> str:
        return str(self.to_frame(unique=True, states=True))

    def to_frame(self, unique: bool = False, states: bool = False) -> pd.DataFrame:
        columns = []
        data = []
        for channel in self:
            if pd.isna(channel.timestamp):
                continue
            channel_uid = channel.key if not unique else channel.id
            channel_data = channel.to_series(state=states)
            channel_data.name = channel_uid

            if channel_data.index.tzinfo is None:
                self._logger.warning(
                    f'UTC will be presumed for channel "{channel.id}" timestamps, '
                    f"as tz-naive with tz-aware DatetimeIndex cannot be joined: {channel_data}"
                )
                channel_data.index = channel_data.index.tz_localize(tz.UTC)

            columns.append(channel_uid)
            data.append(channel_data)
        if len(data) > 0:
            return pd.concat(data, axis="columns", keys=columns)
        return pd.DataFrame(columns=columns)
