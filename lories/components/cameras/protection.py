# -*- coding: utf-8 -*-
"""
lories.components.cameras.protection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from threading import Timer
from typing import Optional, Sequence, Tuple

import pandas as pd
from lories.components.cameras._core import _Camera, _CameraProtector
from lories.connectors.cameras.camera import CameraConnector
from lories.core import Configurations, ResourceError
from lories.core.configs.parameters import DurationParameter
from lories.data import Channel
from lories.util import to_bool


class CameraProtector(_CameraProtector):
    """
    Closes a shutter in front of the camera on motion and re-opens it after ``delay``.

    The camera sees the shutter it drives, so the cycle has to be blind to its own
    movement: every stream channel derived from the camera (all but the raw ``stream``
    view channel) is muted while the shutter is closed and until the ``cooldown`` after
    it opens has passed, so no frames of the shutter reach the motion detector or any
    other consumer.
    """

    _delay = DurationParameter(
        key="delay",
        default="10min",
        desc="Duration the shutter stays closed before auto-opening",
    )
    _cooldown = DurationParameter(
        key="cooldown",
        default="30s",
        desc="Window after the shutter opens where motion is ignored, covering the shutter movement",
    )

    _timer: Optional[Timer] = None
    _unmute_timer: Optional[Timer] = None
    delay: pd.Timedelta
    cooldown: pd.Timedelta
    _cooldown_until: pd.Timestamp = pd.NaT

    @classmethod
    def _assert_context(cls, context: _Camera) -> _Camera:
        if context is None or not isinstance(context, _Camera):
            raise ResourceError(f"Invalid '{cls.__name__}' context: {type(context)}")
        return super()._assert_context(context)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.delay = self._delay
        self.cooldown = self._cooldown

        self.data.add(CameraProtector.STATE, aggregate="max")

    def activate(self) -> None:
        super().activate()
        camera = self.context
        if _Camera.MOTION not in camera.data:
            raise ResourceError(
                f"CameraProtector '{self.id}' requires camera-level 'motion = true' "
                f"to enable the {_Camera.MOTION!s} channel"
            )
        camera.data.register(self._on_motion_detect, _Camera.MOTION, how="any", unique=False)
        self.data.get(CameraProtector.STATE).value = False

    @staticmethod
    def _now() -> pd.Timestamp:
        return pd.Timestamp.now(tz="UTC")

    def close(self, delay: Optional[pd.Timedelta] = None) -> None:
        if delay is not None and delay.total_seconds() > 0:
            if self._timer is not None:
                self._timer.cancel()

            self._timer = Timer(delay.total_seconds(), self.open)
            self._timer.daemon = True
            self._timer.start()

        if self._unmute_timer is not None:
            # A trigger during the cooldown must not let the pending unmute fire while closed.
            self._unmute_timer.cancel()
            self._unmute_timer = None

        # State first: a concurrently running unmute re-checks it after unmuting.
        state = self.data.get(CameraProtector.STATE)
        state.value = True
        try:
            self._mute_consumers()
        except Exception as e:
            # Muting is secondary; the shutter must close regardless.
            self._logger.error(f"Failed to mute the consumers of camera protection '{self.id}': {e}", exc_info=True)
        state.write(True)
        self._logger.info(f"Closed camera protection '{self.id}'")

    def open(self) -> None:
        self._timer = None
        # Arm before the shutter starts moving; the motion listener runs on another thread.
        self._cooldown_until = self._now() + self.cooldown
        state = self.data.get(CameraProtector.STATE)
        state.value = False
        state.write(False)
        self._logger.info(f"Opened camera protection '{self.id}'")

        # Consumers stay muted until the cooldown ends, so none of them sees the shutter opening.
        cooldown = self.cooldown.total_seconds()
        if cooldown > 0:
            self._unmute_timer = Timer(cooldown, self._unmute_consumers)
            self._unmute_timer.daemon = True
            self._unmute_timer.start()
        else:
            self._unmute_consumers()

    @staticmethod
    def _is_consumer(channel: Channel) -> bool:
        # The raw view channel stays live; everything else streamed off the camera is a consumer.
        return to_bool(channel.get("stream", default=False)) and channel.key != _Camera.STREAM

    def _consumer_streams(self) -> Sequence[Tuple[CameraConnector, Channel]]:
        """The camera's derived stream channels, each with the connector streaming it."""
        streams = []
        # Iterating the data access yields channel ids; filter() yields the channels.
        for channel in self.context.data.filter(self._is_consumer):
            connector = channel.connector
            if connector is None or not connector.enabled:
                continue
            connector = connector._connector
            if isinstance(connector, CameraConnector):
                streams.append((connector, channel))
        return streams

    def _mute_consumers(self) -> None:
        streams = self._consumer_streams()
        if not streams:
            self._logger.warning(f"Found no stream consumers to mute for camera protection '{self.id}'")
            return
        for connector, channel in streams:
            connector.mute(channel)
        muted = [channel.id for _, channel in streams]
        self._logger.info(f"Muted camera consumers while '{self.id}' is closed: {muted}")

    def _unmute_consumers(self) -> None:
        self._unmute_timer = None
        if self.is_closed():
            return
        try:
            streams = self._consumer_streams()
            for connector, channel in streams:
                connector.unmute(channel)
            if streams:
                self._logger.info(f"Unmuted camera consumers, '{self.id}' cooldown over")
            if self.is_closed():
                # A close() slipped in between the check and the unmute; it set its state before muting.
                self._mute_consumers()
        except Exception as e:
            # Runs on the cooldown timer thread, where an exception would only kill that thread.
            self._logger.error(f"Failed to unmute the consumers of camera protection '{self.id}': {e}", exc_info=True)

    def is_closed(self) -> bool:
        state = self.data.get(CameraProtector.STATE)
        return state.is_valid() and bool(state.value)

    def is_in_cooldown(self, now: Optional[pd.Timestamp] = None) -> bool:
        if now is None:
            now = self._now()
        return pd.notna(self._cooldown_until) and now < self._cooldown_until

    def trigger(self) -> None:
        """Manually run the protection cycle: close now, auto-open after ``delay``."""
        self.close(delay=self.delay)

    def _on_motion_detect(self, data: pd.DataFrame) -> None:
        # The motion processor returns SKIP on no-motion frames, so this listener
        # only fires when an actual detection lands on the channel.
        now = data.index[-1]
        if self.is_closed():
            # The camera only sees the shutter while closed; re-closing would restart the delay.
            self._logger.debug(f"Motion ignored, '{self.id}' is closed")
            return
        if self.is_in_cooldown(now):
            remaining = (self._cooldown_until - now).total_seconds()
            self._logger.info(f"Motion ignored, '{self.id}' cooldown active for another {remaining:.1f}s")
            return
        self.close(delay=self.delay)
