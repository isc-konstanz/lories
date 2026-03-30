# -*- coding: utf-8 -*-
"""
lories.components.cameras.protection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from threading import Timer
from typing import Optional

from lories.components.cameras._core import _Camera, _CameraProtector
from lories.connectors.cameras import MotionDetector
from lories.core import Configurations, ResourceError


class CameraProtector(_CameraProtector):
    _timer: Optional[Timer] = None

    delay: int = 600  # Default delay in seconds

    @classmethod
    def _assert_context(cls, context: _Camera) -> _Camera:
        if context is None or not isinstance(context, _Camera):
            raise ResourceError(f"Invalid '{cls.__name__}' context: {type(context)}")
        return super()._assert_context(context)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.delay = configs.get_int("delay", default=CameraProtector.delay)

        self.data.add(CameraProtector.STATE, aggregate="max")

    def activate(self) -> None:
        super().activate()
        camera = self.context
        camera.data.register(self._on_motion_detect, MotionDetector.TYPE, how="any", unique=False)

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

    def _on_motion_detect(self, _) -> None:
        self.close(delay=self.delay)
