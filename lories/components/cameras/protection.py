# -*- coding: utf-8 -*-
"""
lories.components.cameras.protection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from threading import Timer
from typing import Any, Optional

import pandas as pd
from lories.components.cameras._core import _Camera, _CameraProtector
from lories.core import Configurations, ResourceError
from lories.util import to_timedelta


def _seconds(value: Any) -> float:
    """Accept duration strings like '10s' / '5min' or numeric seconds."""
    if isinstance(value, str):
        return to_timedelta(value).total_seconds()
    return float(value)


class CameraProtector(_CameraProtector):
    _timer: Optional[Timer] = None

    delay: float = 600.0  # Seconds the shutter stays closed before auto-opening.
    cooldown: float = 30.0  # Seconds after a close before motion can re-trigger;
    # covers the shutter movement during which the camera sees itself.

    _cooldown_until: pd.Timestamp = pd.NaT

    @classmethod
    def _assert_context(cls, context: _Camera) -> _Camera:
        if context is None or not isinstance(context, _Camera):
            raise ResourceError(f"Invalid '{cls.__name__}' context: {type(context)}")
        return super()._assert_context(context)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.delay = _seconds(configs.get("delay", default=CameraProtector.delay))
        self.cooldown = _seconds(configs.get("cooldown", default=CameraProtector.cooldown))

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

    def close(self, delay: int = 0) -> None:
        if delay > 0:
            if self._timer is not None:
                self._timer.cancel()

            self._timer = Timer(self.delay, self.open)
            self._timer.daemon = True
            self._timer.start()

        self.data.get(CameraProtector.STATE).write(True)
        self._logger.info(f"Closed camera protection '{self.id}'")

    def open(self) -> None:
        self._timer = None
        self.data.get(CameraProtector.STATE).write(False)
        self._logger.info(f"Opened camera protection '{self.id}'")

    def _on_motion_detect(self, data: pd.DataFrame) -> None:
        # The motion processor returns SKIP on no-motion frames, so this listener
        # only fires when an actual detection lands on the channel.
        now = data.index[-1]
        if pd.notna(self._cooldown_until) and now < self._cooldown_until:
            remaining = (self._cooldown_until - now).total_seconds()
            self._logger.info(f"Motion ignored, '{self.id}' cooldown active for another {remaining:.1f}s")
            return
        self._cooldown_until = now + pd.Timedelta(seconds=self.cooldown)
        self.close(delay=self.delay)
