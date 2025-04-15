# -*- coding: utf-8 -*-
"""
lori.connectors.revpi
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Dict, Optional

from revpimodio2 import RevPiModIO

import pandas as pd
import pytz as tz
from lori import Configurations
from lori.connectors import Connector, register_connector_type
from lori.core import Resources
from lori.data import Channel


# noinspection PyShadowingBuiltins, SpellCheckingInspection
@register_connector_type("revpi", "revpi_io", "revpi_aio", "revpi_ro", "revolutionpi")
class RevPiConnector(Connector):
    _core: RevPiModIO
    _cycletime: Optional[int]

    _listeners: Dict[str, RevPiListener]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._listeners = {}

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._cycletime = configs.get_int("cycletime", default=None)

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        self._core = RevPiModIO(autorefresh=True)
        if self._cycletime:
            self._core.cycletime = self._cycletime

        # Handle SIGINT / SIGTERM to exit program cleanly
        # self._core.handlesignalend(self._core.cleanup)

        # TODO: register listeners
        # self._core.io[""].reg_event(None, as_thread=True, prefire=True)

        # TODO: set all IO output values to optional default attribute value

    def disconnect(self) -> None:
        super().disconnect()
        # TODO: unregister listeners
        # .unreg_event([func=None, edge=None])

        # TODO: set all IO output values to optional default attribute value

        self._core.cleanup()

    def read(self, resources: Resources) -> pd.DataFrame:
        now = pd.Timestamp.now(tz=tz.UTC)
        data = pd.DataFrame(columns=[r.id for r in resources])
        for resource in resources:
            resource_io = self._core.io[resource.address]
            data.loc[now, resource.id] = resource_io.value
        return data

    def write(self, data: pd.DataFrame) -> None:
        for channel in self.channels:
            if channel.id not in data.columns:
                continue
            channel_data = data.loc[:, channel.id].dropna(axis="index", how="all")
            if channel_data.empty:
                continue

            channel_io = self._core.io[channel.address]
            channel_io.value = channel_data.iloc[-1]


class RevPiListener:
    channel: Channel

    def __call__(self) -> None:
        pass
