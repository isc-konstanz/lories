# -*- coding: utf-8 -*-
"""
lories.connectors.inference.models
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Torch-format model loading (``.pth``/``.pt``, ``.pt2``) for the ``ai_torch``
inference connector; ONNX serving lives in the torch-free ``ai_onnx``
connector. Public API: :class:`Model` and :class:`ModelError`.
"""

from . import model  # noqa: F401
from .model import Model  # noqa: F401
from .utils import ModelError  # noqa: F401
