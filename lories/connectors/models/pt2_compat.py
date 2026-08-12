# -*- coding: utf-8 -*-

"""
lories.connectors.models.pt2_compat
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CPU-only compatibility shims for loading a ``.pt2`` (``torch.export``) graph that was
exported on a CUDA machine. Both hacks are no-ops when CUDA is available.

"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import patch as _mock_patch

import torch


def _load_pt2(path: str, extra_files: Dict[str, str]) -> Any:
    try:
        import torchvision  # noqa: F401
    except ImportError:
        pass

    if torch.cuda.is_available():
        return torch.export.load(path, extra_files=extra_files)

    from torch._export.serde.serialize import deserialize_device as _deserialize_device

    def _cpu_device(d: Any) -> torch.device:
        resolved = _deserialize_device(d)
        return torch.device("cpu") if resolved.type == "cuda" else resolved

    _torch_load = torch.load

    def _cpu_load(*args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("map_location", torch.device("cpu"))
        return _torch_load(*args, **kwargs)

    with _mock_patch("torch.export.pt2_archive._package.deserialize_device", _cpu_device), \
         _mock_patch("torch.load", _cpu_load):
        return torch.export.load(path, extra_files=extra_files)


def _degpu_graph(exported: Any) -> Any:
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
