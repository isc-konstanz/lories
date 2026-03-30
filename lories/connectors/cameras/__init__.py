# -*- coding: utf-8 -*-
"""
lories.connectors.cameras
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from .motion import MotionDetector  # noqa: F401

from .camera import CameraConnector  # noqa: F401

from .stream import (  # noqa: F401
    CameraStream,
    CameraStreamError,
    CameraStreamUnavailableError,
)

from . import opencv  # noqa: F401
from .opencv import OpenCV  # noqa: F401
