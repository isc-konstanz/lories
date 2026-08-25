# -*- coding: utf-8 -*-
"""
tests.test_connectors_inference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``InferenceConnector`` subtype: registration of the ``ai_torch``/``ai_onnx``
types, weights validation at configure, connect/infer round trips with tiny
models, the shared process-wide inference gate (consistency + reset), and the
serves-no-channels contract (channel rejection, raising read/write).
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

import numpy as np

torch = pytest.importorskip("torch")

from lories.connectors import ConnectionError, ConnectorError, registry  # noqa: E402
from lories.connectors.inference import InferenceConnector  # noqa: E402
from lories.core import Resources  # noqa: E402
from lories.core.configs.errors import ConfigurationError  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_gate():
    """The inference gate is process-global class state; isolate tests."""
    yield
    InferenceConnector._InferenceConnector__gate = None
    InferenceConnector._InferenceConnector__gate_limit = None


@pytest.fixture
def tiny_pt2(tmp_path):
    export_api = getattr(torch, "export", None)
    if export_api is None or not hasattr(export_api, "export"):
        pytest.skip("torch.export is not available in this torch build")

    class _Linear(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.linear = torch.nn.Linear(3, 2)

        def forward(self, x):
            return self.linear(x)

    model = _Linear()
    model.eval()
    example = torch.randn(1, 3)
    with torch.no_grad():
        expected = model(example)
    exported = torch.export.export(model, (example,))
    path = tmp_path / "model.pt2"
    torch.export.save(exported, str(path))
    return str(path).replace("\\", "/"), example.numpy(), expected.numpy()


def _boot_application(tmp_path, connectors_conf: str):
    from lories.application import Settings
    from lories.application.main import Application

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir(exist_ok=True)
    (conf_dir / "settings.conf").write_text(
        'name = "inference_test"\n' 'action = "run"\n' "\n" "[interface]\n" "enabled = false\n" "\n" + connectors_conf
    )

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        settings = Settings("inference_test")
        app = Application(settings)
        app.configure(settings)
    finally:
        os.chdir(cwd)
    return app


def _get_connector(app, key: str):
    for connector in app._connectors.values():
        if connector.key == key:
            return connector
    raise AssertionError(f"connector '{key}' not found in {list(app._connectors.values())}")


def test_types_registered():
    assert registry.has_type("ai_torch")
    assert registry.has_type("ai_onnx")
    for _type in ("ai_torch", "ai_onnx"):
        assert issubclass(registry.from_type(_type).type, InferenceConnector)


def test_torch_pt2_connect_and_infer(tmp_path, tiny_pt2):
    path, example, expected = tiny_pt2
    app = _boot_application(
        tmp_path,
        "[connectors.apples]\n" 'type = "ai_torch"\n' f'weights = "{path}"\n' 'device = "cpu"\n',
    )
    connector = _get_connector(app, "apples")

    connector.connect(Resources())
    assert connector.is_connected()
    assert isinstance(connector.meta, dict)

    result = connector.infer(example)
    np.testing.assert_allclose(result, expected, rtol=1e-5, atol=1e-6)

    connector.disconnect()
    assert not connector.is_connected()


def test_onnx_connect_and_infer(tmp_path, tiny_pt2):
    pytest.importorskip("onnxruntime")

    class _Linear(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.linear = torch.nn.Linear(3, 2)

        def forward(self, x):
            return self.linear(x)

    model = _Linear()
    model.eval()
    example = torch.randn(1, 3)
    with torch.no_grad():
        expected = model(example).numpy()
    onnx_path = str(tmp_path / "model.onnx").replace("\\", "/")
    # dynamo=False: the classic tracing exporter; the dynamo path needs onnxscript
    torch.onnx.export(model, (example,), onnx_path, input_names=["x"], dynamo=False)

    app = _boot_application(
        tmp_path,
        "[connectors.oranges]\n" 'type = "ai_onnx"\n' f'weights = "{onnx_path}"\n' "threads = 1\n",
    )
    connector = _get_connector(app, "oranges")

    connector.connect(Resources())
    result = connector.infer(example.numpy())
    np.testing.assert_allclose(result[0], expected, rtol=1e-4, atol=1e-5)


def test_missing_weights_file_fails_configure(tmp_path):
    with pytest.raises(ConfigurationError, match="weights"):
        _boot_application(
            tmp_path,
            "[connectors.apples]\n" 'type = "ai_torch"\n' 'weights = "./does/not/exist.pt2"\n',
        )


def test_wrong_suffix_fails_configure(tmp_path, tiny_pt2):
    path, _, _ = tiny_pt2
    with pytest.raises(ConfigurationError, match="handles"):
        _boot_application(
            tmp_path,
            "[connectors.apples]\n" 'type = "ai_onnx"\n' f'weights = "{path}"\n',
        )


def test_conflicting_max_concurrent_raises(tmp_path, tiny_pt2):
    path, _, _ = tiny_pt2
    with pytest.raises(ConfigurationError, match="max_concurrent"):
        _boot_application(
            tmp_path,
            "[connectors.apples]\n"
            'type = "ai_torch"\n'
            f'weights = "{path}"\n'
            "max_concurrent = 1\n"
            "\n"
            "[connectors.pears]\n"
            'type = "ai_torch"\n'
            f'weights = "{path}"\n'
            "max_concurrent = 2\n",
        )


def test_infer_unconnected_raises(tmp_path, tiny_pt2):
    path, example, _ = tiny_pt2
    app = _boot_application(
        tmp_path,
        "[connectors.apples]\n" 'type = "ai_torch"\n' f'weights = "{path}"\n',
    )
    connector = _get_connector(app, "apples")
    with pytest.raises(ConnectionError):
        connector.infer(example)


def test_channel_binding_rejected(tmp_path, tiny_pt2):
    path, _, _ = tiny_pt2
    app = _boot_application(
        tmp_path,
        "[connectors.apples]\n" 'type = "ai_torch"\n' f'weights = "{path}"\n',
    )
    connector = _get_connector(app, "apples")
    bound = [SimpleNamespace(id="camera.frame")]
    with pytest.raises(ConfigurationError, match="serves no channels"):
        connector._run_connect(bound)


def test_read_write_raise(tmp_path, tiny_pt2):
    path, _, _ = tiny_pt2
    app = _boot_application(
        tmp_path,
        "[connectors.apples]\n" 'type = "ai_torch"\n' f'weights = "{path}"\n',
    )
    connector = _get_connector(app, "apples")
    with pytest.raises(ConnectorError, match="serves no channels"):
        connector._run_read(Resources())
    with pytest.raises(ConnectorError, match="serves no channels"):
        connector._run_write(None)
