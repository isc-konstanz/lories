# -*- coding: utf-8 -*-
"""
lories.components.cameras._camera
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from lories.components import Component
from lories.core import Constant


class _CameraProtector(Component):
    """
    Base class for a camera protection shutter, declaring its state channel.
    """

    TYPE: str = "protection"

    STATE = Constant(bool, "state", "Camera Protection State", context="camera_protection")


class _Camera(Component):
    """
    Base class for camera components, declaring the frame, stream and motion channels.
    """

    TYPE: str = "camera"
    INCLUDES = ["stream", _CameraProtector.TYPE]

    FRAME = Constant(bytes, "frame", "Frame")
    STREAM = Constant(bytes, "stream", "Stream")
    MOTION = Constant(bytes, "motion", "Motion-detected frame")
