# -*- coding: utf-8 -*-
"""
lories.connectors.revpi
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Dict, Optional

import pandas as pd
import pytz as tz
from lories.connectors import Connector, register_connector_type
from lories.core.configs.parameters import Parameter
from lories.data.channels import Channel
from lories.typing import Resources
from lories.util import to_bool

_AVAILABLE = True
_IMPORT_ERROR = None

try:
    from revpimodio2 import EventCallback, RevPiModIO, io
except ImportError as _e:
    _AVAILABLE = False
    _IMPORT_ERROR = f"Missing dependency: revpimodio2 — pip install revpimodio2 ({_e})"
    EventCallback = None  # type: ignore
    RevPiModIO = None  # type: ignore
    io = None  # type: ignore


# noinspection PyShadowingBuiltins, SpellCheckingInspection
@register_connector_type("revpi", "revpi_io", "revpi_aio", "revpi_mio", "revpi_ro", "revolutionpi")
class RevPiConnector(Connector):
    """
    Revolution Pi is a KUNBUS open-source industrial PC platform based on the Raspberry Pi Compute Module. It
    exposes digital and analog I/O through a shared process image, accessible via the revpimodio2 library. The
    modular hardware design supports various I/O expansion modules (DIO, AIO, MIO, RO). However, the process
    image interface is Linux-specific and requires direct hardware access, limiting remote or cross-platform usage.
    """

    __available__ = _AVAILABLE
    __import_error__ = _IMPORT_ERROR

    _cycletime = Parameter(key="cycletime", type=int, required=False, desc="Cycle time override (ms)")

    _core: RevPiModIO
    _cycletime: Optional[int]

    _listeners: Dict[str, RevPiListener]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._listeners = {}

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
