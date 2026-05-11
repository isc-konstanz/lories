# -*- coding: utf-8 -*-
"""
lories.components.camera
~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from typing import TypeVar

from lories.components import Component, register_component_type
from lories.core import Configurations


# noinspection SpellCheckingInspection
@register_component_type("camera")
class Camera(Component):
    """
    A camera component captures still frames from an underlying camera connector and exposes them as a
    bytes-typed channel for downstream consumers. It abstracts over the specific capture backend (e.g.
    RTSP via OpenCV) so that motion detection, snapshot logging, or visual inspection workflows can be
    composed against a uniform interface. Frames are aggregated with last-value semantics, reflecting
    that consumers typically care about the most recent image rather than a continuous stream.
    """

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self.data.add(
            key="frame",
            name="Frame",
            type=bytes,
            aggregation="last",
        )

    # def activate(self) -> None:
    #     super().activate()
    #     self.data.register(
    #         self._on_frame,
    #         self.data.frame,
    #         how="all",
    #         unique=False,
    #     )
    #
    # def _on_frame(self, data: pd.DataFrame) -> None:
    #     pass


CameraType = TypeVar("CameraType", bound=Camera)
