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

        if configs.has_member(CameraProtector.TYPE, includes=True):
            self.protection = CameraProtector(
                self,
                name=f"{self.name} Protection",
                configs=configs.get_member(CameraProtector.TYPE),
            )
            self.components.add(self.protection)

        if configs.get_bool("frame", default=True):
            self.data.add(Camera.FRAME, aggregate="last")

        if configs.get_bool("stream", default=False):
            self.data.add(Camera.STREAM, aggregate="last", freq=None, stream=True)

        if configs.get_bool("motion", default=False):
            self.data.add(
                Camera.MOTION,
                aggregate="last",
                freq=None,
                stream=True,
                processors={"motion": {"processor": "motion"}},
            )

    def has_protection(self) -> bool:
        return self.protection is not None and self.protection.is_enabled()
