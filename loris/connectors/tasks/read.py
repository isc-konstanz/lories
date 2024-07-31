# -*- coding: utf-8 -*-
"""
loris.connectors.tasks.read
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from typing import Optional

import pandas as pd
from loris.connectors.tasks.task import ConnectorTask
from loris.data.channels import ChannelState


class ReadTask(ConnectorTask):
    def run(
        self,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None
    ) -> None:
        self._logger.debug(
            f"Reading {len(self.channels)} channels of " f"{type(self.connector).__name__}: " f"{self.connector.uuid}"
        )
        data = self.connector.read(self.channels, start, end)

        for channel in self.channels:
            if channel.uuid not in data.columns or all(pd.isna(data.loc[:, channel.uuid])):
                channel.state = ChannelState.NOT_AVAILABLE
                self._logger.warning(
                    f"Unable to read nonexisting channel: {channel.uuid}"
                )
                continue

            channel_data = data.loc[:, channel.uuid].dropna()
            channel_data.name = channel.id
            if len(channel_data.index) > 1:
                channel.set(channel_data.index[0], channel_data)

            elif len(channel_data.index) > 0:
                timestamp = channel_data.index[-1]
                channel.set(timestamp, channel_data[timestamp])
