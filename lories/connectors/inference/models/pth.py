# -*- coding: utf-8 -*-
"""
lories.connectors.inference.models.pth
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Runner for ``.pth``/``.pt`` checkpoints: either a fully pickled
``torch.nn.Module`` or a bare state_dict, which requires a configured
``model_builder`` to reconstruct the architecture first.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional, Union

import torch

from lories.connectors.inference.models.utils import ModelError, _resolve_builder, _to_device

_logger = logging.getLogger(__name__)


class _PthRunner:
    def __init__(
        self,
        path: str,
        device: torch.device,
        model_builder: Optional[Union[str, Callable[..., Any]]] = None,
        build_args: Optional[Dict[str, Any]] = None,
        strict: bool = True,
        num_threads: Optional[int] = None,
    ) -> None:
        if num_threads is not None:
            # Process-global: caps torch's intra-op pool for every model in
            # this process, not just this one.
            torch.set_num_threads(num_threads)
        checkpoint = torch.load(path, map_location=device, weights_only=False)
        if isinstance(checkpoint, torch.nn.Module):
            model = checkpoint
        else:
            builder = _resolve_builder(model_builder)
            if builder is None:
                raise ModelError(
                    f"{path!r} holds a state_dict; a 'model_builder' must be configured to "
                    f"reconstruct its architecture before the weights can be loaded"
                )
            model = builder(**(build_args or {}))
            incompatible = model.load_state_dict(checkpoint, strict=strict)
            if not strict:
                if incompatible.missing_keys:
                    _logger.warning("Missing keys: %s", incompatible.missing_keys)
                if incompatible.unexpected_keys:
                    _logger.warning("Unexpected keys: %s", incompatible.unexpected_keys)
        model.eval()
        model.to(device)
        self.model = model
        self.device = device
        self.meta: Dict[str, Any] = {}

    def predict(self, tensor: torch.Tensor) -> Any:
        inp = _to_device(tensor, self.device)
        with torch.no_grad():
            return self.model([inp])
