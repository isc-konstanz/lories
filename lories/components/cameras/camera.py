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

    preview: bool
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

        # ParameterGroup.resolve returns None when the [channels] section is
        # absent and the group has children; treat that as "use all defaults".
        channels = self._channels.resolve(configs) or {}

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
