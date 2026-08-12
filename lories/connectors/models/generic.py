# -*- coding: utf-8 -*-
"""
lories.connectors.models.generic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

import numpy as np
import torch

from lories.connectors.models.pt2_compat import _degpu_graph, _load_pt2
from lories.connectors.models.utils import ModelError, _resolve_builder, _resolve_device, _to_device, _to_numpy_tree

_logger = logging.getLogger(__name__)


class _PthRunner:

    def __init__(
        self,
        path: str,
        device: torch.device,
        model_builder: Optional[Union[str, Callable[..., Any]]] = None,
        build_args: Optional[Dict[str, Any]] = None,
        strict: bool = True,
    ) -> None:
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


class _Pt2Runner:

    def __init__(self, path: str, device: torch.device) -> None:
        extra_files = {"export_meta.json": ""}
        exported = _load_pt2(path, extra_files)
        if not torch.cuda.is_available():
            exported = _degpu_graph(exported)
        self.meta: Dict[str, Any] = json.loads(extra_files["export_meta.json"]) if extra_files["export_meta.json"] else {}
        self.model = exported.module().to(device)
        self.device = device

    def predict(self, tensor: torch.Tensor) -> Any:
        inp = _to_device(tensor, self.device)
        with torch.no_grad():
            return self.model(inp)


class _OnnxRunner:

    def __init__(self, path: str, device: torch.device) -> None:
        try:
            import onnxruntime as ort
        except ImportError as exc:
            raise ModelError(
                "Loading an .onnx model requires onnxruntime (or onnxruntime-gpu for CUDA). "
                "Install with: pip install onnxruntime"
            ) from exc

        providers = (
            ["CUDAExecutionProvider", "CPUExecutionProvider"] if device.type == "cuda" else ["CPUExecutionProvider"]
        )
        self.session = ort.InferenceSession(path, providers=providers)
        self.input_name = self.session.get_inputs()[0].name
        self.device = device

        active = self.session.get_providers()
        if device.type == "cuda" and "CUDAExecutionProvider" not in active:
            _logger.warning(
                "Requested CUDA but this ONNX Runtime session is running on %s; install "
                "onnxruntime-gpu (matching your CUDA version) to use the GPU",
                active[0],
            )

        raw_meta = dict(self.session.get_modelmeta().custom_metadata_map)
        self.meta: Dict[str, Any] = {}
        for key, value in raw_meta.items():
            try:
                self.meta[key] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                self.meta[key] = value

    def predict(self, tensor: torch.Tensor) -> Any:
        array = tensor.detach().cpu().numpy() if isinstance(tensor, torch.Tensor) else np.asarray(tensor)
        return self.session.run(None, {self.input_name: array})


_RUNNERS = {
    ".pth": _PthRunner,
    ".pt": _PthRunner,
    ".pt2": _Pt2Runner,
    ".onnx": _OnnxRunner,
}


class Model:
   
    def __init__(
        self,
        path: str,
        device: str = "auto",
        model_builder: Optional[Union[str, Callable[..., Any]]] = None,
        build_args: Optional[Dict[str, Any]] = None,
        strict: bool = True,
    ) -> None:
        file = Path(path)
        if not file.is_file():
            raise FileNotFoundError(f"Model weights not found: {path}")

        ext = file.suffix.lower()
        runner_cls = _RUNNERS.get(ext)
        if runner_cls is None:
            raise ModelError(f"Unsupported model format {ext!r}; expected one of {sorted(_RUNNERS)}")

        self._device = _resolve_device(device)
        if runner_cls is _PthRunner:
            self._runner = _PthRunner(str(file), self._device, model_builder, build_args, strict)
        else:
            self._runner = runner_cls(str(file), self._device)

        self.meta: Dict[str, Any] = dict(getattr(self._runner, "meta", {}))
        _logger.info("Model loaded from %s (%s) on %s", path, ext, self._device)

    @property
    def device(self) -> torch.device:
        return self._device

    def predict(self, tensor: torch.Tensor) -> Any:
        return _to_numpy_tree(self._runner.predict(tensor))
