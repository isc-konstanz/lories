# -*- coding: utf-8 -*-
"""
lories.components.cameras._camera
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from lories.components import Component
from lories.core import Constant


class _CameraProtector(Component):
    TYPE: str = "protection"

    STATE = Constant(bool, "state", "Camera protection state", alias="protection_state")


class _Camera(Component):
    TYPE: str = "camera"
    INCLUDES = ["stream", _CameraProtector.TYPE]

    FRAME = Constant(bytes, "frame", "Frame")
    STREAM = Constant(bytes, "stream", "Stream")
    MOTION = Constant(bytes, "motion", "Motion-detected frame")
