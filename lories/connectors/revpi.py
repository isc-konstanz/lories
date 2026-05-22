# -*- coding: utf-8 -*-
"""
lories.connectors.revpi
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Dict, Optional

from revpimodio2 import EventCallback, RevPiModIO
from revpimodio2.io import IntIO

import pandas as pd
import pytz as tz
from lories import Configurations
from lories.connectors import Connector, register_connector_type
from lories.data.channels import Channel
from lories.typing import Resources
from lories.util import to_bool, to_timedelta


# noinspection PyShadowingBuiltins, SpellCheckingInspection
@register_connector_type("revpi", "revpi_io", "revpi_aio", "revpi_mio", "revpi_ro", "revolutionpi")
class RevPiConnector(Connector):
    """
    Revolution Pi is a KUNBUS open-source industrial PC platform based on the Raspberry Pi Compute Module. It
    exposes digital and analog I/O through a shared process image, accessible via the revpimodio2 library. The
    modular hardware design supports various I/O expansion modules (DIO, AIO, MIO, RO). However, the process
    image interface is Linux-specific and requires direct hardware access, limiting remote or cross-platform usage.
    """

    # Parameter / ChannelParameter descriptors disabled for now — the names
    # aren't imported in this file, so the class definition would NameError
    # (which surfaces as a ModuleNotFoundError-style failure when the
    # connector module is import-loaded). The runtime already pulls each of
    # these via configs.get_int("cycletime", ...) / channel.get("listener", ...)
    # / channel.get("address") / channel.get("edge", ...) / channel.get("cooldown", ...)
    # below, so behavior is unchanged.
    # TODO: re-enable once `Parameter` / `ChannelParameter` are imported from
    #       lories.core.configs.parameters.
    #
    # _cycletime = Parameter(
    #     key="cycletime",
    #     type=int,
    #     required=False,
    #     min=1,
    #     desc="Process image polling cycle time in milliseconds (defaults to the RevPiModIO library default)",
    # )
    #
    # # Per-channel parameters
    # address = ChannelParameter(type=str, required=True, desc="RevPi process image I/O address name")
    # listener = ChannelParameter(
    #     type=bool,
    #     required=False,
    #     default=False,
    #     desc="Register a rising-edge event listener that pushes updates as they occur",
    # )
    # edge = ChannelParameter(
    #     type=str,
    #     required=False,
    #     default=None,
    #     choices=["rising", "falling", "both"],
    #     desc=(
    #         "Edge filter for bit-IO listeners: 'rising' | 'falling' | 'both'. "
    #         "Only applies to bit-oriented IOs (digital inputs / single bits). "
    #         "Omit for counter / analog / byte / word IOs — revpimodio2 rejects 'edge' on "
    #         "non-bit objects and instead fires on any value change. "
    #         "If 'edge' is configured on a non-bit IO it is ignored (with a warning) and the "
    #         "listener fires on any change."
    #     ),
    # )
    # cooldown = ChannelParameter(
    #     type=str,
    #     required=False,
    #     default=None,
    #     desc=(
    #         "Minimum interval between successive RevPiListener firings for this channel "
    #         "(duration string, e.g. '5s', '500ms', '1min'). Events that arrive within "
    #         "the cooldown window after the last accepted event are dropped."
    #     ),
    # )

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
            cooldown_raw = channel.get("cooldown", None)
            cooldown = to_timedelta(cooldown_raw) if cooldown_raw else None
            channel_listener = RevPiListener(channel, cooldown=cooldown)
            channel_io = self._core.io[channel_listener.address]

            event_kwargs = {"as_thread": True, "prefire": True}
            edge = channel.get("edge", None)
            if edge is not None:
                # revpimodio2 raises RuntimeError("parameter 'edge' can be used with bit io objects only")
                # when 'edge' is passed for an IntIO / StructIO (counters, analog, byte/word IOs).
                # Detect non-bit IOs and silently drop the kwarg instead of crashing the connector.
                if isinstance(channel_io, IntIO):
                    self._logger.warning(
                        f"Ignoring edge='{edge}' on non-bit IO '{channel_listener.address}' "
                        f"({type(channel_io).__name__}) for channel '{channel.id}' — "
                        f"revpimodio2 only supports edge filtering on bit IOs. "
                        f"Listener will fire on any value change."
                    )
                else:
                    event_kwargs["edge"] = self._EDGES[str(edge).lower()]

            channel_io.reg_event(channel_listener, **event_kwargs)
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
    _cooldown: Optional[pd.Timedelta]
    _last_fired: Optional[pd.Timestamp]

    def __init__(
        self,
        channel: Channel,
        cooldown: Optional[pd.Timedelta] = None,
    ):
        self._channel = channel
        self.address = channel.address
        self._cooldown = cooldown
        self._last_fired = None

    def __call__(self, event: EventCallback) -> None:
        now = pd.Timestamp.now(tz=tz.UTC).floor(freq="s")
        if self._cooldown is not None and self._last_fired is not None:
            if now - self._last_fired < self._cooldown:
                return
        self._channel.set(now, event.iovalue)
        self._last_fired = now
