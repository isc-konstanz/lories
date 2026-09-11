# -*- coding: utf-8 -*-
"""
lories.tests.test_connectors_sunspec
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unit tests for the SunSpec connector against synthetic in-memory register images:
a pysunspec2 model set builds the byte image ('SunS' marker + models 1/103/123 for
an inverter, 1/203 for a meter, each with an end-model sentinel), and the TCP client
device's transport methods are monkeypatched to serve reads and writes from the image
belonging to the requested Modbus unit id — so the real base-address scan, model-chain
walk, scale-factor application, and not-implemented sentinel handling all run without
sockets, for several units behind one connector.

The connector is instantiated via ``__new__`` (the ``Registrator`` construction
path needs a full application context), which also bypasses the
``ConnectorMeta`` method wrapping, so the raw connect/read/write methods are
driven directly. A slow test runs the same round trip through a real pymodbus
TCP server on localhost serving two unit ids.
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
import sunspec2.modbus.modbus as sunspec_modbus  # noqa: E402

from lories.connectors.errors import ConnectionError  # noqa: E402
from lories.connectors.sunspec import SunSpecClient  # noqa: E402
from lories.core.configs import ConfigurationError  # noqa: E402
from lories.core.resource import Resource  # noqa: E402
from lories.core.resources import Resources  # noqa: E402
from lories.data.channels import ChannelState  # noqa: E402

BASE_ADDRESS = 40000

INVERTER_UNIT = 126
METER_UNIT = 127
ABSENT_UNIT = 200

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


def _build_meter_image() -> bytearray:
    common = _load_model(1)
    common.points["Mn"].value = "MeterCo"
    common.points["Md"].value = "TestMeter"
    common.points["SN"].value = "0815"

    meter = _load_model(203)
    meter.points["W"].value = 2500
    meter.points["W_SF"].value = 0

    return bytearray(b"SunS" + common.get_mb() + meter.get_mb() + b"\xff\xff\x00\x00")


class _ImageTransport:
    """Serves pysunspec2 device reads/writes from per-unit in-memory register images."""

    def __init__(self, images: dict) -> None:
        self.images = dict(images)
        # Unit ids that raise a Modbus timeout instead of answering, i.e. a device whose
        # DC side went down while the transport itself is fine
        self.silent = set()
        self.reads = []
        self.writes = []

    def read(self, slave_id: int, addr: int, count: int, op=None) -> bytes:
        self.reads.append((slave_id, addr, count))
        if slave_id in self.silent:
            raise sunspec_modbus.ModbusClientTimeout("Response timeout")
        image = self.images.get(slave_id)
        if image is None:
            # Nothing on this unit id: the scan's base-address probe reads nothing back
            return b""
        offset = (addr - BASE_ADDRESS) * 2
        return bytes(image[offset : offset + count * 2])

    def write(self, slave_id: int, addr: int, data: bytes) -> None:
        self.writes.append((slave_id, addr, len(data)))
        image = self.images[slave_id]
        offset = (addr - BASE_ADDRESS) * 2
        image[offset : offset + len(data)] = data

    def reads_of(self, slave_id: int) -> list:
        return [r for r in self.reads if r[0] == slave_id]


@pytest.fixture
def transport(monkeypatch) -> _ImageTransport:
    stub = _ImageTransport({INVERTER_UNIT: _build_image(), METER_UNIT: _build_meter_image()})
    device_class = sunspec_modbus_client.SunSpecModbusClientDeviceTCP
    monkeypatch.setattr(device_class, "connect", lambda self, timeout=None: None)
    monkeypatch.setattr(device_class, "disconnect", lambda self: None)
    monkeypatch.setattr(device_class, "is_connected", lambda self: True)
    monkeypatch.setattr(device_class, "read", lambda self, addr, count, op=None: stub.read(self.slave_id, addr, count))
    monkeypatch.setattr(device_class, "write", lambda self, addr, data: stub.write(self.slave_id, addr, data))
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
    client._timeout = pd.Timedelta("3s")
    client._Connector__resources = resources
    return client


INVERTER_RESOURCES = (
    {"id": "ac_power", "type": float, "device": INVERTER_UNIT, "model": "103", "point": "W"},
    {"id": "frequency", "type": float, "device": INVERTER_UNIT, "model": "103", "point": "Hz"},
    {"id": "manufacturer", "type": str, "device": INVERTER_UNIT, "model": "common", "point": "Mn"},
)

METER_RESOURCES = ({"id": "grid_power", "type": float, "device": METER_UNIT, "model": "203", "point": "W"},)


def _resolved(client: SunSpecClient) -> dict:
    return client._SunSpecClient__resolved


def _devices(client: SunSpecClient) -> dict:
    return client._SunSpecClient__devices


# ---------------------------------------------------------------- connect / lazy scan


def test_connect_opens_transport_without_touching_devices(transport):
    resources = _make_resources(*INVERTER_RESOURCES, *METER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)

    assert client.is_connected()
    # Scanning is deferred to the first read, so a device asleep at startup cannot keep
    # the transport, or its siblings, from coming up
    assert transport.reads == []
    assert _devices(client) == {}


def test_read_scans_each_unit_once_and_caches_points(transport):
    resources = _make_resources(
        *INVERTER_RESOURCES,
        *METER_RESOURCES,
        {"id": "bogus", "type": float, "device": INVERTER_UNIT, "model": "103", "point": "NoSuchPoint"},
    )
    client = _make_client(resources)
    client.connect(resources)

    client.read(resources)
    assert set(_devices(client)) == {INVERTER_UNIT, METER_UNIT}
    # Unresolvable points are cached as None so the warning is not repeated every read
    assert _resolved(client)[INVERTER_UNIT]["bogus"] is None
    assert _resolved(client)[INVERTER_UNIT]["ac_power"] is not None

    transport.reads.clear()
    client.read(resources)
    # The second read issues block reads only, no base-address probing: model 103 and
    # model 1 on the inverter, model 203 on the meter
    assert len(transport.reads) == 3
    assert len(transport.reads_of(INVERTER_UNIT)) == 2
    assert len(transport.reads_of(METER_UNIT)) == 1


def test_disconnect_releases_every_device(transport):
    resources = _make_resources(*INVERTER_RESOURCES, *METER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)
    client.read(resources)
    assert len(_devices(client)) == 2

    client.disconnect()
    assert _devices(client) == {}
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


def test_read_groups_by_device_across_units(transport):
    resources = _make_resources(*INVERTER_RESOURCES, *METER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)

    row = client.read(resources).iloc[0]
    assert row["ac_power"] == 1500.0
    assert row["grid_power"] == 2500.0


def test_read_maps_unimplemented_and_unresolved_to_not_available(transport):
    resources = _make_resources(
        {"id": "ac_current", "type": float, "device": INVERTER_UNIT, "model": "103", "point": "A"},
        {"id": "bogus", "type": float, "device": INVERTER_UNIT, "model": "103", "point": "NoSuchPoint"},
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
    client.read(resources)

    transport.reads.clear()
    client.read(resources)

    # ac_power + frequency share the model 103 block; manufacturer reads model 1
    assert len(transport.reads) == 2


def test_read_without_device_key_is_argument_syntax_error(transport):
    resources = _make_resources({"id": "orphan", "type": float, "model": "103", "point": "W"})
    client = _make_client(resources)
    client.connect(resources)

    row = client.read(resources).iloc[0]
    assert row["orphan"] == ChannelState.ARGUMENT_SYNTAX_ERROR
    assert client.is_connected()


def test_read_maps_model_error_to_unknown_error(transport):
    resources = _make_resources(*INVERTER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)
    client.read(resources)

    class _BrokenPoint:
        @property
        def cvalue(self):
            raise sunspec_device.ModelError("scale factor not found")

    model, _ = _resolved(client)[INVERTER_UNIT]["ac_power"]
    _resolved(client)[INVERTER_UNIT]["ac_power"] = (model, _BrokenPoint())

    row = client.read(resources).iloc[0]
    assert row["ac_power"] == ChannelState.UNKNOWN_ERROR
    # The rest of the model block is unaffected
    assert row["frequency"] == 49.99


# ---------------------------------------------------------------- per-unit isolation


def test_absent_unit_is_benched_without_dropping_the_connector(transport):
    resources = _make_resources(
        *METER_RESOURCES,
        {"id": "ghost", "type": float, "device": ABSENT_UNIT, "model": "103", "point": "W"},
    )
    client = _make_client(resources)
    client.connect(resources)

    row = client.read(resources).iloc[0]
    assert row["ghost"] == ChannelState.NOT_AVAILABLE
    # A unit that will not scan is a device problem, not a transport problem
    assert row["grid_power"] == 2500.0
    assert client.is_connected()
    assert ABSENT_UNIT in client._SunSpecClient__unavailable


def test_silent_unit_degrades_only_its_own_channels(transport):
    resources = _make_resources(*INVERTER_RESOURCES, *METER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)
    client.read(resources)

    # The inverter's DC side goes down: it stops answering, the meter beside it does not
    transport.silent.add(INVERTER_UNIT)
    row = client.read(resources).iloc[0]

    assert row["ac_power"] == ChannelState.UNKNOWN_ERROR
    assert row["frequency"] == ChannelState.UNKNOWN_ERROR
    assert row["grid_power"] == 2500.0
    assert client.is_connected()


def test_benched_unit_costs_no_bus_time_until_the_interval_passes(transport):
    resources = _make_resources(*INVERTER_RESOURCES, *METER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)
    client.read(resources)

    transport.silent.add(INVERTER_UNIT)
    client.read(resources)

    transport.reads.clear()
    row = client.read(resources).iloc[0]

    assert transport.reads_of(INVERTER_UNIT) == [], "benched unit was retried within the interval"
    assert transport.reads_of(METER_UNIT), "healthy unit stopped being read"
    assert row["grid_power"] == 2500.0


def test_benched_unit_recovers_once_the_interval_passes(transport):
    resources = _make_resources(*INVERTER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)
    client.read(resources)

    transport.silent.add(INVERTER_UNIT)
    client.read(resources)
    assert INVERTER_UNIT in client._SunSpecClient__unavailable

    # The inverter wakes up, and the bench interval has passed
    transport.silent.discard(INVERTER_UNIT)
    client._interval_reconnect = pd.Timedelta(0)

    row = client.read(resources).iloc[0]
    assert row["ac_power"] == 1500.0
    assert INVERTER_UNIT not in client._SunSpecClient__unavailable


def test_device_timeout_does_not_mark_the_connector_unhealthy(transport):
    resources = _make_resources(*INVERTER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)
    client.read(resources)

    transport.silent.add(INVERTER_UNIT)
    client.read(resources)

    # A unit that stops answering must not trip the framework's reconnect gate
    assert client.is_connected()


def test_transport_error_marks_unhealthy(transport, monkeypatch):
    resources = _make_resources(*INVERTER_RESOURCES)
    client = _make_client(resources)
    client.connect(resources)
    client.read(resources)
    assert client.is_connected()

    def raise_transport_error(self, addr, count, op=None):
        raise sunspec_modbus.ModbusClientError("Client serial port not open")

    monkeypatch.setattr(sunspec_modbus_client.SunSpecModbusClientDeviceTCP, "read", raise_transport_error)
    with pytest.raises(ConnectionError):
        client.read(resources)

    # The RTU device's is_connected() is a hardcoded True stub, so the health flag
    # must make the framework's reconnect gate trip after a transport error
    assert not client.is_connected()


# ---------------------------------------------------------------- write path


def test_write_applies_inverse_scale_factor(transport, monkeypatch):
    resources = _make_resources(
        {"id": "power_limit", "type": float, "device": INVERTER_UNIT, "model": "123", "point": "WMaxLimPct"}
    )
    client = _make_client(resources)
    client.connect(resources)

    monkeypatch.setattr(SunSpecClient, "channels", property(lambda self: resources))

    timestamp = pd.Timestamp.now(tz="UTC").floor(freq="s")
    client.write(pd.DataFrame({"power_limit": [95.0]}, index=[timestamp]))

    assert transport.writes, "no registers were written"
    assert client.read(resources).iloc[0]["power_limit"] == 95.0
    # WMaxLimPct_SF is -2, so the register must hold the unscaled raw value
    _, point = _resolved(client)[INVERTER_UNIT]["power_limit"]
    assert point.value == 9500


def test_write_addresses_the_channel_own_unit(transport, monkeypatch):
    resources = _make_resources(
        {"id": "power_limit", "type": float, "device": INVERTER_UNIT, "model": "123", "point": "WMaxLimPct"}
    )
    client = _make_client(resources)
    client.connect(resources)
    monkeypatch.setattr(SunSpecClient, "channels", property(lambda self: resources))

    timestamp = pd.Timestamp.now(tz="UTC").floor(freq="s")
    client.write(pd.DataFrame({"power_limit": [95.0]}, index=[timestamp]))

    assert {slave for slave, _, _ in transport.writes} == {INVERTER_UNIT}


# ---------------------------------------------------------------- point-path resolution


def _make_resolver_client() -> SunSpecClient:
    client = SunSpecClient.__new__(SunSpecClient)
    client._logger = logging.getLogger("test_connectors_sunspec")
    return client


def test_resolve_point_walks_repeating_groups():
    target = object()
    module_2 = SimpleNamespace(groups={}, points={"DCW": target})
    module_1 = SimpleNamespace(groups={}, points={"DCW": object()})
    model = SimpleNamespace(groups={"module": [module_1, module_2]}, points={})
    device = SimpleNamespace(models={160: [model], "mppt": [model]})
    client = _make_resolver_client()

    resource = Resource(id="dcw", key="dcw", name="dcw", type=float, model="160", point="module.2.DCW")
    resolved_model, resolved_point = client._resolve_point(device, resource)
    assert resolved_model is model
    assert resolved_point is target

    by_name = Resource(id="dcw", key="dcw", name="dcw", type=float, model="mppt", point="module.2.DCW")
    assert client._resolve_point(device, by_name)[1] is target


def test_resolve_point_selects_model_instance():
    first, second = (SimpleNamespace(groups={}, points={"W": object()}) for _ in range(2))
    device = SimpleNamespace(models={203: [first, second]})
    client = _make_resolver_client()

    resource = Resource(id="w", key="w", name="w", type=float, model="203", point="W", instance=2)
    assert client._resolve_point(device, resource)[0] is second


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
    device = SimpleNamespace(models={160: [mppt], 203: [meter]})
    client = _make_resolver_client()

    resource = Resource(id="bad", key="bad", name="bad", type=float, **spec)
    with pytest.raises(ConfigurationError):
        client._resolve_point(device, resource)


# ---------------------------------------------------------------- loopback integration


@pytest.mark.slow
def test_tcp_roundtrip_against_pymodbus_server():
    pymodbus_server = pytest.importorskip("pymodbus.server")

    import socket
    import threading
    import time

    from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext, ModbusSlaveContext

    def _slave(image: bytearray) -> ModbusSlaveContext:
        words = [int.from_bytes(image[i : i + 2], "big") for i in range(0, len(image), 2)]
        # pymodbus 3.9's slave context always adds 1 to the request address before hitting the block
        return ModbusSlaveContext(hr=ModbusSequentialDataBlock(BASE_ADDRESS + 1, words))

    context = ModbusServerContext(
        slaves={INVERTER_UNIT: _slave(_build_image()), METER_UNIT: _slave(_build_meter_image())},
        single=False,
    )

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

    resources = _make_resources(*INVERTER_RESOURCES, *METER_RESOURCES)
    client = _make_client(resources)
    client._host = "127.0.0.1"
    client._port = port

    client.connect(resources)
    try:
        row = client.read(resources).iloc[0]
        assert row["ac_power"] == 1500.0
        assert row["frequency"] == 49.99
        assert row["manufacturer"] == "TestCo"
        # The meter answers on its own unit id behind the same gateway
        assert row["grid_power"] == 2500.0
    finally:
        client.disconnect()
