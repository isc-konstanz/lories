# -*- coding: utf-8 -*-
"""
tests.test_connectors_models
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Per-format unit tests for the ``lories.connectors.inference.models`` torch loader
(``.pth``/``.pt``, ``.pt2``; ONNX serving lives in the ``ai_onnx`` connector).
Guarded so missing torch skips rather than kills collection (issue 04).

"""

from __future__ import annotations

import importlib

import pytest

import numpy as np


@pytest.fixture(scope="module")
def torch_mod():
    return pytest.importorskip("torch")


@pytest.fixture(scope="module")
def models_utils(torch_mod):
    return importlib.import_module("lories.connectors.inference.models.utils")


@pytest.fixture(scope="module")
def models_pkg(torch_mod):
    return importlib.import_module("lories.connectors.inference.models")


@pytest.fixture(scope="module")
def list_linear_cls(torch_mod):
    # torch.save(model) pickles the class BY REFERENCE (module + qualname), so the
    # class cannot be local to a test function. It also cannot be defined at module
    # top level (no unconditional torch import allowed), so define it once here and
    # register it under a stable qualname in the module globals for pickle to find.
    torch = torch_mod

    class _ListLinear(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.linear = torch.nn.Linear(3, 2)

        def forward(self, xs):
            # _PthRunner.predict calls self.model([inp]) -- forward receives a list.
            return self.linear(xs[0])

    _ListLinear.__qualname__ = "_ListLinear"
    globals()["_ListLinear"] = _ListLinear
    return _ListLinear


# ---------------------------------------------------------------- error paths


def test_unsupported_extension_raises_model_error(tmp_path, torch_mod, models_pkg, models_utils):
    path = tmp_path / "weights.txt"
    path.write_text("not a model")

    with pytest.raises(models_utils.ModelError):
        models_pkg.Model(str(path))


def test_missing_file_raises_file_not_found(tmp_path, torch_mod, models_pkg):
    path = tmp_path / "missing.pth"

    with pytest.raises(FileNotFoundError):
        models_pkg.Model(str(path))


# ---------------------------------------------------------------- .pth / .pt


def test_pth_full_module_round_trip(tmp_path, torch_mod, models_pkg, list_linear_cls):
    torch = torch_mod

    model = list_linear_cls()
    model.eval()
    example = torch.randn(1, 3)
    with torch.no_grad():
        expected = model([example])

    path = tmp_path / "model.pth"
    torch.save(model, str(path))

    loaded = models_pkg.Model(str(path))
    result = loaded.predict(example)

    assert isinstance(result, np.ndarray)
    np.testing.assert_allclose(result, expected.numpy(), rtol=1e-5, atol=1e-6)


def test_pth_state_dict_without_builder_raises(tmp_path, torch_mod, models_pkg, models_utils, list_linear_cls):
    torch = torch_mod

    model = list_linear_cls()
    path = tmp_path / "model.pth"
    torch.save(model.state_dict(), str(path))

    with pytest.raises(models_utils.ModelError):
        models_pkg.Model(str(path))


def test_pth_state_dict_with_builder_reconstructs(tmp_path, torch_mod, models_pkg, list_linear_cls):
    torch = torch_mod

    model = list_linear_cls()
    model.eval()
    example = torch.randn(1, 3)
    with torch.no_grad():
        expected = model([example])

    path = tmp_path / "model.pth"
    torch.save(model.state_dict(), str(path))

    # model_builder passed as a direct callable (not a dotted-path string).
    loaded = models_pkg.Model(str(path), model_builder=list_linear_cls)
    result = loaded.predict(example)

    # A fresh, unloaded _ListLinear() has independently random weights; matching the
    # saved model's output to tight tolerance pins that the state_dict was actually applied.
    np.testing.assert_allclose(result, expected.numpy(), rtol=1e-5, atol=1e-6)


# ---------------------------------------------------------------- builder / device resolution


def test_resolve_builder_variants(torch_mod, models_utils):
    resolved = models_utils._resolve_builder("torch.nn.Identity")
    assert callable(resolved)
    assert resolved is torch_mod.nn.Identity

    with pytest.raises(models_utils.ModelError):
        models_utils._resolve_builder("no.such.thing")

    assert models_utils._resolve_builder(None) is None


def test_resolve_device_cpu_outcome(torch_mod, models_utils):
    torch = torch_mod

    auto_device = models_utils._resolve_device("auto")
    if torch.cuda.is_available():
        assert auto_device.type == "cuda"
    else:
        assert auto_device.type == "cpu"

    # "cuda" without CUDA available must fall back to CPU rather than raise.
    cuda_device = models_utils._resolve_device("cuda")
    if torch.cuda.is_available():
        assert cuda_device.type == "cuda"
    else:
        assert cuda_device.type == "cpu"


# ---------------------------------------------------------------- .pt2 (torch.export)


def test_pt2_export_round_trip(tmp_path, torch_mod, models_pkg):
    torch = torch_mod
    export_api = getattr(torch, "export", None)
    if export_api is None or not hasattr(export_api, "export"):
        pytest.skip("torch.export is not available in this torch build")

    class _Linear(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.linear = torch.nn.Linear(3, 2)

        def forward(self, x):
            # _Pt2Runner.predict calls self.model(inp) directly -- a plain tensor, no list.
            return self.linear(x)

    model = _Linear()
    model.eval()
    example = torch.randn(1, 3)
    with torch.no_grad():
        expected = model(example)

    exported = torch.export.export(model, (example,))
    path = tmp_path / "model.pt2"
    torch.export.save(exported, str(path))

    loaded = models_pkg.Model(str(path))
    result = loaded.predict(example)

    np.testing.assert_allclose(result, expected.numpy(), rtol=1e-5, atol=1e-6)


# ---------------------------------------------------------------- .onnx


def test_onnx_redirects_to_ai_onnx_connector(tmp_path, torch_mod, models_pkg, models_utils):
    # ONNX serving is retired from the torch loader; the torch-free ai_onnx
    # connector covers it
    path = tmp_path / "model.onnx"
    path.write_bytes(b"unused")

    with pytest.raises(models_utils.ModelError, match="ai_onnx"):
        models_pkg.Model(str(path))


# ---------------------------------------------------------------- _to_numpy_tree


def test_to_numpy_tree_converts_nested_structures(torch_mod, models_utils):
    torch = torch_mod

    tree = {
        "a": torch.tensor([1.0, 2.0]),
        "b": [torch.tensor([3.0]), (torch.tensor([4.0]), torch.tensor([5.0]))],
    }
    result = models_utils._to_numpy_tree(tree)

    assert isinstance(result["a"], np.ndarray)
    np.testing.assert_allclose(result["a"], [1.0, 2.0])

    assert isinstance(result["b"], list)
    assert isinstance(result["b"][0], np.ndarray)
    assert isinstance(result["b"][1], tuple)
    assert isinstance(result["b"][1][0], np.ndarray)
    np.testing.assert_allclose(result["b"][1][1], [5.0])
