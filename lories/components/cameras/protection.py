# -*- coding: utf-8 -*-
"""
lories.components.cameras.protection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from threading import Timer
from typing import Optional

import pandas as pd
from lories.components.cameras._core import _Camera, _CameraProtector
from lories.core import Configurations, ResourceError
from lories.core.configs.parameters import DurationParameter


class CameraProtector(_CameraProtector):
    """
    Closes a shutter in front of the camera on motion and re-opens it after ``delay``.

    The camera sees the shutter it drives, so the cycle has to be blind to its own
    movement: motion while the shutter is closed is ignored, and ``cooldown`` starts
    when the shutter opens, covering the opening movement and the scene reacquisition.
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

        state = self.data.get(CameraProtector.STATE)
        state.value = True
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
