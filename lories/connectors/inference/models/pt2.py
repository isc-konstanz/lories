# -*- coding: utf-8 -*-
"""
lories.connectors.inference.models.pt2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Runner for ``.pt2`` (``torch.export``) archives, including the CPU
compatibility shims needed to load a graph that was exported on a CUDA
machine: ``torch.export`` freezes device placement into the serialized
program, so on a CPU-only machine both the deserialization (``_load_archive``)
and the literal ``cuda`` devices baked into the FX graph (``_graph_to_cpu``)
have to be remapped. Both shims are no-ops when CUDA is available.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional
from unittest.mock import patch as _mock_patch

import torch

from lories.connectors.inference.models.utils import _to_device

_logger = logging.getLogger(__name__)


def _load_archive(path: str, extra_files: Dict[str, str]) -> Any:
    # Best-effort: importing torchvision registers its custom ops (e.g. the
    # in-graph RPN NMS of a Faster R-CNN export) before deserialization.
    try:
        import torchvision  # noqa: F401

        torchvision_missing = False
    except ImportError:
        torchvision_missing = True

    def _reraise_with_hint(exc: Exception) -> Any:
        if torchvision_missing:
            raise RuntimeError(
                f"Failed deserializing '{path}': the graph may reference torchvision ops "
                f"(e.g. torch.ops.torchvision.nms), and torchvision is not installed"
            ) from exc
        raise exc

    if torch.cuda.is_available():
        try:
            return torch.export.load(path, extra_files=extra_files)
        except Exception as e:
            return _reraise_with_hint(e)

    from torch._export.serde.serialize import deserialize_device as _deserialize_device

    def _cpu_device(d: Any) -> torch.device:
        resolved = _deserialize_device(d)
        return torch.device("cpu") if resolved.type == "cuda" else resolved

    _torch_load = torch.load

    def _cpu_load(*args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("map_location", torch.device("cpu"))
        return _torch_load(*args, **kwargs)

    with (
        _mock_patch("torch.export.pt2_archive._package.deserialize_device", _cpu_device),
        _mock_patch("torch.load", _cpu_load),
    ):
        try:
            return torch.export.load(path, extra_files=extra_files)
        except Exception as e:
            return _reraise_with_hint(e)


def _graph_to_cpu(exported: Any) -> Any:
    graph = exported.graph_module.graph
    changed = False
    for node in graph.nodes:
        for key, value in list(node.kwargs.items()):
            if isinstance(value, torch.device) and value.type == "cuda":
                node.update_kwarg(key, torch.device("cpu"))
                changed = True
        for i, value in enumerate(node.args):
            if isinstance(value, torch.device) and value.type == "cuda":
                node.update_arg(i, torch.device("cpu"))
                changed = True
    if changed:
        exported.graph_module.recompile()
    return exported


class _Pt2Runner:
    def __init__(self, path: str, device: torch.device, num_threads: Optional[int] = None) -> None:
        if num_threads is not None:
            # Process-global: caps torch's intra-op pool for every model in
            # this process, not just this one.
            torch.set_num_threads(num_threads)
        extra_files = {"export_meta.json": ""}
        exported = _load_archive(path, extra_files)
        if not torch.cuda.is_available():
            exported = _graph_to_cpu(exported)
        meta_json = extra_files["export_meta.json"]
        self.meta: Dict[str, Any] = json.loads(meta_json) if meta_json else {}
        self.model = exported.module().to(device)
        self.device = device

    def predict(self, tensor: torch.Tensor) -> Any:
        inp = _to_device(tensor, self.device)
        with torch.no_grad():
            return self.model(inp)
