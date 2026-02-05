# -*- coding: utf-8 -*-
"""
lories.connectors.cameras.motion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from lories.core import Configurator
from lories.data import Channels


class MotionDetector(Configurator):
    TYPE: str = "motion"

    __channels: Channels

    def __init__(self, channels: Channels, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__channels = channels

    def __call__(self, frame: bytes) -> None:
        if self.detect(frame):
            self._logger.info("Detected motion in camera frame")
            for channel in self.__channels:
                channel.value = frame

    def detect(self, frame: bytes) -> bool:
        # TODO: Implement motion detection algorithm
        return False

    def is_enabled(self) -> bool:
        return super().is_enabled() and len(self.__channels) > 0
