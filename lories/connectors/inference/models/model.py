# -*- coding: utf-8 -*-
"""
lories.connectors.inference.models.model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Torch-format model loading: dispatches on the weight file's extension to
the matching runner and normalizes predictions to numpy structures. ONNX
serving lives in the torch-free ``ai_onnx`` connector, not here.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

import torch

from lories.connectors.inference.models.pt2 import _Pt2Runner
from lories.connectors.inference.models.pth import _PthRunner
from lories.connectors.inference.models.utils import ModelError, _resolve_device, _to_numpy_tree

_logger = logging.getLogger(__name__)

_RUNNERS = {
    ".pth": _PthRunner,
    ".pt": _PthRunner,
    ".pt2": _Pt2Runner,
}


class Model:
    """A loaded inference model, dispatched by weight-file extension.

    Supports pickled modules or state_dicts (``.pth``/``.pt``, the latter
    reconstructed via ``model_builder``) and ``torch.export`` archives
    (``.pt2``). ``predict`` returns numpy structures.

    ``num_threads`` caps the runtime's intra-op compute threads (default: the
    runtime's own default, usually all cores) via ``torch.set_num_threads``,
    which is process-global.
    """

    def __init__(
        self,
        path: str,
        device: str = "auto",
        model_builder: Optional[Union[str, Callable[..., Any]]] = None,
        build_args: Optional[Dict[str, Any]] = None,
        strict: bool = True,
        num_threads: Optional[int] = None,
    ) -> None:
        file = Path(path)
        if not file.is_file():
            raise FileNotFoundError(f"Model weights not found: {path}")

        ext = file.suffix.lower()
        if ext == ".onnx":
            raise ModelError(
                "ONNX weights are served by the torch-free 'ai_onnx' connector; "
                'use type = "ai_onnx" instead of the torch loader'
            )
        runner_cls = _RUNNERS.get(ext)
        if runner_cls is None:
            raise ModelError(f"Unsupported model format {ext!r}; expected one of {sorted(_RUNNERS)}")

        self._device = _resolve_device(device)
        if runner_cls is _PthRunner:
            self._runner = _PthRunner(str(file), self._device, model_builder, build_args, strict, num_threads)
        else:
            self._runner = runner_cls(str(file), self._device, num_threads)

        self.meta: Dict[str, Any] = dict(getattr(self._runner, "meta", {}))
        _logger.info("Model loaded from %s (%s) on %s", path, ext, self._device)

    @property
    def device(self) -> torch.device:
        return self._device

    def predict(self, tensor: torch.Tensor) -> Any:
        return _to_numpy_tree(self._runner.predict(tensor))
