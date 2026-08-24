# -*- coding: utf-8 -*-
"""
lories.tests.test_connectors_sunspec
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unit tests for the SunSpec connector against a synthetic in-memory register
image: a pysunspec2 model set builds the byte image ('SunS' marker + models
1/103/123 + end-model sentinel), and the TCP client device's transport methods
are monkeypatched to serve reads and writes from that image — so the real
base-address scan, model-chain walk, scale-factor application, and
not-implemented sentinel handling all run without sockets.

The connector is instantiated via ``__new__`` (the ``Registrator`` construction
path needs a full application context), which also bypasses the
``ConnectorMeta`` method wrapping, so the raw connect/read/write methods are
driven directly. A slow test runs the same round trip through a real pymodbus
TCP server on localhost.
"""

from __future__ import annotations

import json
import logging
import os
from types import SimpleNamespace

import pytest

import pandas as pd

sunspec2 = pytest.importorskip("sunspec2")

import sunspec2.device as sunspec_device  # noqa: E402
import sunspec2.modbus.client as sunspec_modbus_client  # noqa: E402

from lories.connectors.errors import ConnectionError  # noqa: E402
from lories.connectors.sunspec import SunSpecClient  # noqa: E402
from lories.core.configs import ConfigurationError  # noqa: E402
from lories.core.resource import Resource  # noqa: E402
from lories.core.resources import Resources  # noqa: E402
from lories.data.channels import ChannelState  # noqa: E402

BASE_ADDRESS = 40000

MODEL_DIR = os.path.join(os.path.dirname(sunspec2.__file__), "models", "json")


def _load_model(model_id: int) -> sunspec_device.Model:
    with open(os.path.join(MODEL_DIR, f"model_{model_id}.json")) as file:
        return sunspec_device.Model(model_def=json.load(file))


def _build_image() -> bytearray:
    common = _load_model(1)
    common.points["Mn"].value = "TestCo"
    common.points["Md"].value = "TestInv"
    common.points["SN"].value = "4711"

    inverter = _load_model(103)
    inverter.points["W"].value = 15000
    inverter.points["W_SF"].value = -1
    inverter.points["Hz"].value = 4999
    inverter.points["Hz_SF"].value = -2
    # 'A' is left unset: packs as the not-implemented sentinel (0xFFFF)

    controls = _load_model(123)
    controls.points["WMaxLimPct_SF"].value = -2

    return bytearray(b"SunS" + common.get_mb() + inverter.get_mb() + controls.get_mb() + b"\xff\xff\x00\x00")


class _ImageTransport:
    """Serves pysunspec2 device reads/writes from an in-memory register image."""

    def __init__(self, image: bytearray) -> None:
        self.image = image
        self.reads = []
        self.writes = []

    def read(self, addr: int, count: int, op=None) -> bytes:
        self.reads.append((addr, count))
        offset = (addr - BASE_ADDRESS) * 2
        return bytes(self.image[offset : offset + count * 2])

    def write(self, addr: int, data: bytes) -> None:
        self.writes.append((addr, len(data)))
        offset = (addr - BASE_ADDRESS) * 2
        self.image[offset : offset + len(data)] = data


@pytest.fixture
def transport(monkeypatch) -> _ImageTransport:
    stub = _ImageTransport(_build_image())
    device_class = sunspec_modbus_client.SunSpecModbusClientDeviceTCP
    monkeypatch.setattr(device_class, "connect", lambda self, timeout=None: None)
    monkeypatch.setattr(device_class, "disconnect", lambda self: None)
    monkeypatch.setattr(device_class, "is_connected", lambda self: True)
    monkeypatch.setattr(device_class, "read", lambda self, addr, count, op=None: stub.read(addr, count))
    monkeypatch.setattr(device_class, "write", lambda self, addr, data: stub.write(addr, data))
    return stub


def _make_resources(*specs: dict) -> Resources:
    resources = []
    for spec in specs:
        configs = {k: v for k, v in spec.items() if k != "id"}
        resources.append(Resource(id=spec["id"], key=spec["id"], name=spec["id"], **configs))
    return Resources(resources)


def _make_client(resources: Resources) -> SunSpecClient:
    client = SunSpecClient.__new__(SunSpecClient)
    client._logger = logging.getLogger("test_connectors_sunspec")
    client._protocol = "tcp"
    client._host = "sunspec.test"
    client._port = 502
    client._device_id = 1
    client._timeout = pd.Timedelta("3s")
    client._Connector__resources = resources
    return client


INVERTER_RESOURCES = (
    {"id": "ac_power", "type": float, "model": "103", "point": "W"},
    {"id": "frequency", "type": float, "model": "103", "point": "Hz"},
    {"id": "manufacturer", "type": str, "model": "common", "point": "Mn"},
)


# ---------------------------------------------------------------- connect / discovery


def test_connect_scans_and_resolves_points(transport):
    resources = _make_resources(
        *INVERTER_RESOURCES,
        {"id": "bogus", "type": float, "model": "103", "point": "NoSuchPoint"},
    )
    client = _make_client(resources)
    client.connect(resources)

    resolved = client._SunSpecClient__points
    assert set(resolved) == {"ac_power", "frequency", "manufacturer"}
    assert "bogus" not in resolved


def test_connect_without_sunspec_map_raises_connection_error(transport):
    transport.image = bytearray(b"\x00" * len(transport.image))
    resources = _make_resources(*INVERTER_RESOURCES)
    client = _make_client(resources)

    with pytest.raises(ConnectionError):
        client.connect(resources)
    assert client._SunSpecClient__device is None
    assert not client.is_connected()


def test_disconnect_releases_device(transport):
    resources = _make_resources(*INVERTER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)
    assert client.is_connected()

    client.disconnect()
    assert client._SunSpecClient__device is None
    assert not client.is_connected()


# ---------------------------------------------------------------- read paths


def test_read_applies_scale_factors_and_strings(transport):
    resources = _make_resources(*INVERTER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)

    data = client.read(resources)

    assert len(data.index) == 1
    row = data.iloc[0]
    assert row["ac_power"] == 1500.0
    assert row["frequency"] == 49.99
    assert row["manufacturer"] == "TestCo"


def test_read_maps_unimplemented_and_unresolved_to_not_available(transport):
    resources = _make_resources(
        {"id": "ac_current", "type": float, "model": "103", "point": "A"},
        {"id": "bogus", "type": float, "model": "103", "point": "NoSuchPoint"},
    )
    client = _make_client(resources)
    client.connect(resources)

    row = client.read(resources).iloc[0]
    assert row["ac_current"] == ChannelState.NOT_AVAILABLE
    assert row["bogus"] == ChannelState.NOT_AVAILABLE


def test_read_issues_one_block_read_per_model(transport):
    resources = _make_resources(*INVERTER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)

    transport.reads.clear()
    client.read(resources)

    # ac_power + frequency share the model 103 block; manufacturer reads model 1
    assert len(transport.reads) == 2


def test_read_transport_error_marks_unhealthy(transport, monkeypatch):
    resources = _make_resources(*INVERTER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)
    assert client.is_connected()

    def raise_timeout(self, addr, count, op=None):
        raise sunspec_modbus_client.SunSpecModbusClientTimeout("no response")

    monkeypatch.setattr(sunspec_modbus_client.SunSpecModbusClientDeviceTCP, "read", raise_timeout)
    with pytest.raises(ConnectionError):
        client.read(resources)

    # The RTU device's is_connected() is a hardcoded True stub, so the health flag
    # must make the framework's reconnect gate trip after a transport error
    assert not client.is_connected()


def test_read_maps_model_error_to_unknown_error(transport):
    resources = _make_resources(*INVERTER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)

    class _BrokenPoint:
        @property
        def cvalue(self):
            raise sunspec_device.ModelError("scale factor not found")

    client._SunSpecClient__points["ac_power"] = _BrokenPoint()

    row = client.read(resources).iloc[0]
    assert row["ac_power"] == ChannelState.UNKNOWN_ERROR
    # The rest of the model block is unaffected
    assert row["frequency"] == 49.99


# ---------------------------------------------------------------- write path


def test_write_applies_inverse_scale_factor(transport, monkeypatch):
    resources = _make_resources({"id": "power_limit", "type": float, "model": "123", "point": "WMaxLimPct"})
    client = _make_client(resources)
    client.connect(resources)

    # Channel construction needs a full task context; the write loop only uses ids
    monkeypatch.setattr(SunSpecClient, "channels", property(lambda self: [SimpleNamespace(id="power_limit")]))

    timestamp = pd.Timestamp.now(tz="UTC").floor(freq="s")
    client.write(pd.DataFrame({"power_limit": [95.0]}, index=[timestamp]))

    assert transport.writes, "no registers were written"
    assert client.read(resources).iloc[0]["power_limit"] == 95.0
    # WMaxLimPct_SF is -2, so the register must hold the unscaled raw value
    point = client._SunSpecClient__points["power_limit"]
    assert point.value == 9500


# ---------------------------------------------------------------- point-path resolution


def _make_resolver_client(models: dict) -> SunSpecClient:
    client = SunSpecClient.__new__(SunSpecClient)
    client._logger = logging.getLogger("test_connectors_sunspec")
    client._SunSpecClient__device = SimpleNamespace(models=models)
    return client


def test_resolve_point_walks_repeating_groups():
    target = object()
    module_2 = SimpleNamespace(groups={}, points={"DCW": target})
    module_1 = SimpleNamespace(groups={}, points={"DCW": object()})
    model = SimpleNamespace(groups={"module": [module_1, module_2]}, points={})
    client = _make_resolver_client({160: [model], "mppt": [model]})

    resource = Resource(id="dcw", key="dcw", name="dcw", type=float, model="160", point="module.2.DCW")
    resolved_model, resolved_point = client._resolve_point(resource)
    assert resolved_model is model
    assert resolved_point is target

    by_name = Resource(id="dcw", key="dcw", name="dcw", type=float, model="mppt", point="module.2.DCW")
    assert client._resolve_point(by_name)[1] is target


def test_resolve_point_selects_model_instance():
    first, second = (SimpleNamespace(groups={}, points={"W": object()}) for _ in range(2))
    client = _make_resolver_client({203: [first, second]})

    resource = Resource(id="w", key="w", name="w", type=float, model="203", point="W", instance=2)
    assert client._resolve_point(resource)[0] is second


@pytest.mark.parametrize(
    "spec",
    [
        {"model": "999", "point": "W"},  # model not on device
        {"model": "203", "point": "W", "instance": 3},  # instance out of range
        {"model": "160", "point": "module.DCW"},  # repeating group without index
        {"model": "160", "point": "module.9.DCW"},  # repeating index out of range
        {"model": "203", "point": "NoSuchPoint"},  # unknown point
    ],
)
def test_resolve_point_rejects_invalid_paths(spec):
    module = SimpleNamespace(groups={}, points={"DCW": object()})
    mppt = SimpleNamespace(groups={"module": [module]}, points={})
    meter = SimpleNamespace(groups={}, points={"W": object()})
    client = _make_resolver_client({160: [mppt], 203: [meter]})

    resource = Resource(id="bad", key="bad", name="bad", type=float, **spec)
    with pytest.raises(ConfigurationError):
        client._resolve_point(resource)


# ---------------------------------------------------------------- loopback integration


@pytest.mark.slow
def test_tcp_roundtrip_against_pymodbus_server():
    pymodbus_server = pytest.importorskip("pymodbus.server")

    import socket
    import threading
    import time

    from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext, ModbusSlaveContext

    image = _build_image()
    words = [int.from_bytes(image[i : i + 2], "big") for i in range(0, len(image), 2)]
    # pymodbus 3.9's slave context always adds 1 to the request address before hitting the block
    block = ModbusSequentialDataBlock(BASE_ADDRESS + 1, words)
    context = ModbusServerContext(slaves=ModbusSlaveContext(hr=block), single=True)

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    server = threading.Thread(
        target=pymodbus_server.StartTcpServer,
        kwargs={"context": context, "address": ("127.0.0.1", port)},
        daemon=True,
    )
    server.start()
    for _ in range(50):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                break
        except OSError:
            time.sleep(0.1)
    else:
        pytest.fail("pymodbus test server did not come up")

    resources = _make_resources(*INVERTER_RESOURCES)
    client = _make_client(resources)
    client._host = "127.0.0.1"
    client._port = port

    client.connect(resources)
    try:
        row = client.read(resources).iloc[0]
        assert row["ac_power"] == 1500.0
        assert row["frequency"] == 49.99
        assert row["manufacturer"] == "TestCo"
    finally:
        client.disconnect()
