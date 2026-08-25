# -*- coding: utf-8 -*-
"""
lories.connectors.inference.models.utils
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

import logging
import pydoc
from typing import Any, Callable, Optional, Union

import torch

from lories.core import ResourceError

_logger = logging.getLogger(__name__)


class ModelError(ResourceError):
    """Raised for generic model loading or inference failures."""


def _resolve_device(device: str) -> torch.device:
    device = device.lower()
    if device == "auto":
        if torch.cuda.is_available():
            _logger.info("CUDA available: %s", torch.cuda.get_device_name(0))
            return torch.device("cuda:0")
        return torch.device("cpu")
    resolved = torch.device(device)
    if resolved.type == "cuda" and not torch.cuda.is_available():
        _logger.warning("CUDA requested but not available; falling back to CPU")
        return torch.device("cpu")
    return resolved


def _resolve_builder(builder: Optional[Union[str, Callable[..., Any]]]) -> Optional[Callable[..., Any]]:
    if builder is None or callable(builder):
        return builder
    resolved = pydoc.locate(builder)
    if resolved is None or not callable(resolved):
        raise ModelError(f"Could not resolve 'model_builder' {builder!r} to a callable")
    return resolved


def _to_device(value: Any, device: torch.device) -> Any:
    if isinstance(value, torch.Tensor):
        return value.to(device)
    if isinstance(value, (list, tuple)):
        return type(value)(_to_device(v, device) for v in value)
    return value


def _to_numpy_tree(value: Any) -> Any:
    if isinstance(value, torch.Tensor):
        return value.detach().cpu().numpy()
    if isinstance(value, dict):
        return {k: _to_numpy_tree(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_to_numpy_tree(v) for v in value)
    return value
