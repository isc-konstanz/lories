# -*- coding: utf-8 -*-
"""
lories.connectors.inference.torch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Torch-backed inference connector for ``.pth``/``.pt``/``.pt2`` weights,
wrapping the per-format runners of :mod:`lories.connectors.inference.models`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import torch

import numpy as np
from lories.connectors.context import register_connector_type
from lories.connectors.errors import ConnectionError
from lories.connectors.inference.inference import InferenceConnector
from lories.connectors.inference.models import Model
from lories.core import Configurations, Resources
from lories.core.configs.errors import ConfigurationError
from lories.core.configs.parameters import Parameter

try:
    import torchvision  # noqa: F401

    _TORCHVISION_ERROR: Optional[str] = None
except ImportError as e:
    # Not fatal: only graphs referencing torchvision ops (e.g. the in-graph
    # RPN NMS of a Faster R-CNN .pt2 export) need it at deserialization time.
    _TORCHVISION_ERROR = str(e)

# torch.set_num_threads is process-global: all ai_torch connectors must agree
# on 'threads', or the last one to load silently rewrites the others' setting.
_TORCH_THREADS: Optional[int] = None


@register_connector_type("ai_torch")
class TorchInferenceConnector(InferenceConnector):
    SUFFIXES = (".pth", ".pt", ".pt2")

    _model_builder = Parameter(
        key="model_builder",
        type=str,
        required=False,
        desc="Dotted path to the model factory; only used for bare state_dict weights",
    )

    _model: Optional[Model] = None
    _build_args: Dict[str, Any]

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        suffix = Path(self._weights).suffix.lower()
        if suffix not in TorchInferenceConnector.SUFFIXES:
            raise ConfigurationError(
                f"'{self.id}' handles {'/'.join(TorchInferenceConnector.SUFFIXES)} weights, got: {self._weights}"
            )

        build_configs = configs.get_member("build", defaults={})
        self._build_args = {k: build_configs[k] for k in build_configs.keys()}

        if self._threads is not None:
            global _TORCH_THREADS
            if _TORCH_THREADS is None:
                _TORCH_THREADS = self._threads
            elif _TORCH_THREADS != self._threads:
                raise ConfigurationError(
                    f"Conflicting 'threads' values for ai_torch connectors: torch.set_num_threads "
                    f"is process-global, {_TORCH_THREADS} is already in effect, '{self.id}' declares "
                    f"{self._threads}. All ai_torch connectors must agree."
                )

        if _TORCHVISION_ERROR is not None:
            self._logger.warning(
                "torchvision is not installed; weights whose graphs reference torchvision ops "
                "(e.g. Faster R-CNN NMS) will fail to load on '%s'",
                self.id,
            )

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        try:
            self._model = Model(
                self._weights,
                device=self._device,
                model_builder=self._model_builder,
                num_threads=self._threads,
                build_args=self._build_args,
            )
        except Exception as exc:
            hint = ""
            if _TORCHVISION_ERROR is not None:
                hint = " (torchvision is not installed; graphs referencing torchvision ops need it)"
            raise ConnectionError(self, f"Failed to load model '{self._weights}'{hint}: {exc}") from exc

    def disconnect(self) -> None:
        self._model = None

    def is_connected(self) -> bool:
        return self._model is not None

    @property
    def meta(self) -> Dict[str, Any]:
        return dict(self._model.meta) if self._model is not None else {}

    def infer(self, inputs: Any, **kwargs) -> Any:
        tensor = inputs if isinstance(inputs, torch.Tensor) else torch.from_numpy(np.ascontiguousarray(inputs))
        return self._model.predict(tensor)
