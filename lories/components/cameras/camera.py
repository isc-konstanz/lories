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
from lories.core.configs.parameters import BoolParameter, ParameterGroup


# noinspection SpellCheckingInspection
@register_component_type("camera")
class Camera(_Camera):
    _channels = ParameterGroup(
        key="channels",
        desc="Channels exposed by this camera",
        children=[
            BoolParameter(key="stream", default=False, desc="Expose a live MJPEG stream channel"),
            BoolParameter(key="motion", default=False, desc="Run motion detection on the stream"),
        ],
    )
    _preview = BoolParameter(
        key="preview",
        default=False,
        desc="Render the live preview in the dashboard",
    )

    preview: bool = False
    protection: Optional[CameraProtector] = None

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.preview = self._preview
        self.protection = CameraProtector(
            self,
            name=f"{self.name} Protection",
            configs=configs.get_member(CameraProtector.TYPE, defaults={}),
        )
        self.components.add(self.protection)

        channels = self._channels

        self.data.add(Camera.FRAME, aggregate="last")

        if channels.get("stream", False):
            self.data.add(Camera.STREAM, aggregate="last", freq=None, stream=True)

        if channels.get("motion", False):
            self.data.add(
                Camera.MOTION,
                aggregate="last",
                freq=None,
                stream=True,
                processors={"motion": {"processor": "motion"}},
            )

    def has_protection(self) -> bool:
        return self.protection is not None and self.protection.is_enabled()

    def is_muted(self) -> bool:
        """
        Whether the protection shutter is closed. Stream channels derived from the camera are muted
        automatically; consumers that poll frames on their own schedule should check this and skip.
        """
        return self.has_protection() and self.protection.is_closed()
