# -*- coding: utf-8 -*-
"""
lories.components.cameras.camera
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from typing import Optional

from lories.components import register_component_type
from lories.components.cameras._core import _Camera
from lories.components.cameras.protection import CameraProtector
from lories.core import Configurations
from lories.core.configs.parameters import BoolParameter, IntParameter, ParameterGroup


# noinspection SpellCheckingInspection
@register_component_type("camera")
class Camera(_Camera):
    """
    Live camera feed exposed as a component.

    Captures frames from a camera connector (such as ``opencv``) and publishes them as data
    channels. A live video stream and per-frame motion detection can each be enabled, and the
    most recent frame can be previewed in the dashboard.

    An optional protection shutter closes the camera when motion is detected and reopens it
    after a delay. It is opt-in through a ``[protection]`` section and requires motion detection
    to be enabled.
    """

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
    _min_size = IntParameter(
        key="min_size",
        default=0,
        min=0,
        desc="Skip captured frames smaller than this many bytes (0 disables)",
    )

    preview: bool = False
    protection: Optional[CameraProtector] = None

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.preview = self._preview

        if configs.has_member(CameraProtector.TYPE, includes=True):
            protection = configs.get_member(CameraProtector.TYPE, defaults={})
            if protection.enabled:
                self.protection = CameraProtector(
                    self,
                    name=f"{self.name} Protection",
                    configs=protection,
                )
                self.components.add(self.protection)

        channels = self._channels

        frame_configs = {}
        if self._min_size > 0:
            frame_configs["processors"] = {"size": {"processor": "size", "min_size": self._min_size}}
        self.data.add(Camera.FRAME, aggregate="last", **frame_configs)

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
        Whether the protection is active: the shutter is closed or its cooldown is still running.
        Stream channels derived from the camera are muted automatically for that whole span;
        consumers that poll frames on their own schedule should check this and skip.
        """
        return self.has_protection() and (self.protection.is_closed() or self.protection.is_in_cooldown())
