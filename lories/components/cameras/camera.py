# -*- coding: utf-8 -*-
"""
lories.components.camera
~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from typing import Optional

from lories.components import register_component_type
from lories.components.cameras._core import _Camera
from lories.components.cameras.protection import CameraProtector
from lories.core import Configurations


# noinspection SpellCheckingInspection
@register_component_type("camera")
class Camera(_Camera):
    protection: Optional[CameraProtector] = None

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.protection = CameraProtector(
            self,
            name=f"{self.name} Protection",
            configs=configs.get_member(CameraProtector.TYPE, defaults={}),
        )
        self.components.add(self.protection)

        self.data.add(Camera.FRAME, aggregation="last")

        if self.protection.is_enabled() or configs.get_bool("stream", default=False):
            self.data.add(Camera.STREAM, aggregation="last", freq=None, stream=True)

        # TODO: Other configuration reason for stream/motion detection to use?
        if self.protection.is_enabled():
            self.data.add(Camera.MOTION, aggregation="last", freq=None, listener=False, motion_detection=True)

    def has_protection(self) -> bool:
        return self.protection is not None and self.protection.is_enabled()
