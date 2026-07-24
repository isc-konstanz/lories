# -*- coding: utf-8 -*-
"""
lories.data.processors
~~~~~~~~~~~~~~~~~~~~~~


"""

from .errors import ProcessingError  # noqa: F401

from .processor import Processor  # noqa: F401

from . import context  # noqa: F401
from .context import (  # noqa: F401
    ProcessorContext,
    register_processor_type,
    registry,
)

from . import differentiator  # noqa: F401
from .differentiator import Differentiator  # noqa: F401

from . import integrator  # noqa: F401
from .integrator import Integrator  # noqa: F401

try:
    import cv2  # noqa: F401
except ImportError:
    # opencv-python is an optional dependency
    pass
else:
    from . import motion  # noqa: F401
    from .motion import MotionDetector  # noqa: F401
