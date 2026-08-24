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
    _delay = DurationParameter(
        key="delay",
        default="10min",
        desc="Duration the shutter stays closed before auto-opening",
    )
    _cooldown = DurationParameter(
        key="cooldown",
        default="30s",
        # Covers the shutter movement during which the camera sees itself.
        desc="Window after a close where motion is ignored before it can re-trigger",
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
        state = self.data.get(CameraProtector.STATE)
        state.value = False
        state.write(False)
        self._logger.info(f"Opened camera protection '{self.id}'")

    def is_in_cooldown(self) -> bool:
        return pd.notna(self._cooldown_until) and pd.Timestamp.now(tz="UTC") < self._cooldown_until

    def trigger(self) -> None:
        """Manually run the protection cycle: close now, auto-open after ``delay``."""
        self._cooldown_until = pd.Timestamp.now(tz="UTC") + self.cooldown
        self.close(delay=self.delay)

    def _on_motion_detect(self, data: pd.DataFrame) -> None:
        # The motion processor returns SKIP on no-motion frames, so this listener
        # only fires when an actual detection lands on the channel.
        now = data.index[-1]
        if pd.notna(self._cooldown_until) and now < self._cooldown_until:
            remaining = (self._cooldown_until - now).total_seconds()
            self._logger.info(f"Motion ignored, '{self.id}' cooldown active for another {remaining:.1f}s")
            return
        self._cooldown_until = now + self.cooldown
        self.close(delay=self.delay)
