# -*- coding: utf-8 -*-
"""
lories.connectors.cameras
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from .camera import CameraConnector  # noqa: F401

from .stream import (  # noqa: F401
    CameraStream,
    CameraStreamError,
    CameraStreamUnavailableError,
)

try:
    from . import opencv  # noqa: F401
    from .opencv import OpenCV  # noqa: F401
except ImportError:
    # opencv-python is an optional dependency
    pass
