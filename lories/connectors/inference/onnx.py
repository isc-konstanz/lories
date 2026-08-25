# -*- coding: utf-8 -*-
"""
lories.connectors.inference.onnx
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ONNX Runtime inference connector for ``.onnx`` weights. Deliberately
self-contained (onnxruntime + numpy only, no torch import), so ONNX-only
deployments run without the torch stack installed.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import onnxruntime as ort

import numpy as np
from lories.connectors.context import register_connector_type
from lories.connectors.errors import ConnectionError
from lories.connectors.inference.inference import InferenceConnector
from lories.core import Configurations, Resources
from lories.core.configs.errors import ConfigurationError


@register_connector_type("ai_onnx")
class OnnxInferenceConnector(InferenceConnector):
    SUFFIXES = (".onnx",)

    _session: Optional[ort.InferenceSession] = None
    _input_name: Optional[str] = None
    _meta: Dict[str, Any]

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        suffix = Path(self._weights).suffix.lower()
        if suffix not in OnnxInferenceConnector.SUFFIXES:
            raise ConfigurationError(
                f"'{self.id}' handles {'/'.join(OnnxInferenceConnector.SUFFIXES)} weights, got: {self._weights}"
            )

    def _resolve_providers(self) -> list[str]:
        device = self._device.lower()
        available = ort.get_available_providers()
        if device == "cpu":
            return ["CPUExecutionProvider"]
        if "CUDAExecutionProvider" in available:
            return ["CUDAExecutionProvider", "CPUExecutionProvider"]
        if device.startswith("cuda"):
            self._logger.warning(
                "Requested CUDA but no CUDAExecutionProvider is available; '%s' runs on CPU "
                "(install onnxruntime-gpu matching your CUDA version to use the GPU)",
                self.id,
            )
        return ["CPUExecutionProvider"]

    def connect(self, resources: Resources) -> None:
        super().connect(resources)

        session_options = ort.SessionOptions()
        if self._threads is not None:
            session_options.intra_op_num_threads = self._threads
            # ORT intra-op threads spin-wait between runs by default, burning
            # whole cores even while idle — poison for a shared process that
            # runs inference on a minutes-scale cadence.
            session_options.add_session_config_entry("session.intra_op.allow_spinning", "0")

        try:
            self._session = ort.InferenceSession(
                self._weights,
                sess_options=session_options,
                providers=self._resolve_providers(),
            )
        except Exception as exc:
            raise ConnectionError(self, f"Failed to load model '{self._weights}': {exc}") from exc
        self._input_name = self._session.get_inputs()[0].name

        self._meta = {}
        for key, value in dict(self._session.get_modelmeta().custom_metadata_map).items():
            try:
                self._meta[key] = json.loads(value)
            except (TypeError, ValueError):
                self._meta[key] = value

    def disconnect(self) -> None:
        self._session = None
        self._input_name = None

    def is_connected(self) -> bool:
        return self._session is not None

    @property
    def meta(self) -> Dict[str, Any]:
        return dict(self._meta) if self._session is not None else {}

    def infer(self, inputs: Any, **kwargs) -> Any:
        array = np.asarray(inputs)
        return self._session.run(None, {self._input_name: array})
