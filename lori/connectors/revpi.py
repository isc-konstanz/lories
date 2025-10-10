# -*- coding: utf-8 -*-
"""
lori.connectors.revpi
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Dict, Optional

from revpimodio2 import EventCallback, RevPiModIO, io

import pandas as pd
import pytz as tz
from lori.connectors import Connector, register_connector_type
from lori.data.channels import Channel
from lori.typing import Configurations, Resources
from lori.util import to_bool


# noinspection PyShadowingBuiltins, SpellCheckingInspection
@register_connector_type("revpi", "revpi_io", "revpi_aio", "revpi_mio", "revpi_ro", "revolutionpi")
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

        channels = resources.filter(lambda r: isinstance(r, Channel) and to_bool(r.get("listener", False)))
        for channel in channels:
            channel_listener = RevPiListener(channel)
            channel_io = self._core.io[channel_listener.address]
            channel_io.reg_event(channel_listener, edge=io.RISING, as_thread=True, prefire=True)
            self._listeners[channel.id] = channel_listener

        # Handle SIGINT / SIGTERM to exit program cleanly
        # self._core.handlesignalend(self._core.cleanup)

        # TODO: set all IO output values to optional default attribute value
        self._core.mainloop(blocking=False)

    def disconnect(self) -> None:
        super().disconnect()
        for listener in self._listeners.values():
            listener_io = self._core.io[listener.address]
            listener_io.unreg_event(listener)

        # TODO: set all IO output values to optional default attribute value

        self._core.cleanup()

    # noinspection PyTypeChecker
    def read(self, resources: Resources) -> pd.DataFrame:
        now = pd.Timestamp.now(tz=tz.UTC).floor(freq="s")
        data = pd.DataFrame(index=[now], columns=resources.ids)
        for resource in resources:
            resource_io = self._core.io[resource.address]
            self._logger.debug(f"Read RevPi IO '{resource_io}': {resource_io.value}")

            data.at[now, resource.id] = resource_io.value
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
    address: str

    _channel: Channel

    def __init__(self, channel: Channel):
        self._channel = channel
        self.address = channel.address

    def __call__(self, event: EventCallback) -> None:
        now = pd.Timestamp.now(tz=tz.UTC).floor(freq="s")
        self._channel.set(now, event.iovalue)
