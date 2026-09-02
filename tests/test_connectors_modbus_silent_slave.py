# -*- coding: utf-8 -*-
"""
lories.tests.test_connectors_modbus_silent_slave
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A Modbus slave that does not answer makes pymodbus RAISE ``ModbusIOException``
(``result.isError()`` covers only protocol exception responses). That used to
escape the per-resource handling and abort the whole connector read, blanking
every channel of every device on the connector (lories-frictions issue 05).
The read now isolates the failure per device group and continues; transport
errors still raise ``ConnectionError``. ``device_id`` is accepted as an alias
for the ``device`` channel key.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pymodbus")

from pymodbus.exceptions import ConnectionException, ModbusIOException  # noqa: E402

from lories._core import ChannelState  # noqa: E402
from lories.connectors.errors import ConnectionError  # noqa: E402
from lories.connectors.modbus.client import ModbusClient  # noqa: E402
from lories.core.resources import Resources  # noqa: E402


class _FakeResource:
    def __init__(self, id, **configs):
        self.id = id
        self._configs = configs

    def get(self, key, default=None):
        return self._configs.get(key, default)


class _FakeResult:
    registers = [42]

    @staticmethod
    def isError() -> bool:
        return False


class _FakePymodbusClient:
    def __init__(self, silent_devices=(), transport_error=False):
        self.silent_devices = set(silent_devices)
        self.transport_error = transport_error
        self.requested_devices = []

    def read_holding_registers(self, address, count, device_id):
        if self.transport_error:
            raise ConnectionException("transport down")
        self.requested_devices.append(device_id)
        if device_id in self.silent_devices:
            raise ModbusIOException("no response received")
        return _FakeResult()

    @staticmethod
    def convert_from_registers(registers, type, word_order):
        return registers[0]


class _ProbeModbusClient(ModbusClient):
    id = "modbus_probe"


def _probe(fake_client, resources) -> ModbusClient:
    probe = _ProbeModbusClient.__new__(_ProbeModbusClient)
    probe._endian = "big"
    probe._ModbusClient__client = fake_client
    probe._ModbusClient__registers = {
        r.id: SimpleNamespace(address=0, length=1, function="holding_register", type=None) for r in resources
    }
    import logging

    probe._logger = logging.getLogger("test.modbus_probe")
    return probe


def test_silent_device_does_not_abort_the_read():
    # The silent device comes FIRST, so the read must survive it to reach device 1
    resources = Resources(
        [
            _FakeResource("weather_temp", device=2),
            _FakeResource("weather_wind", device=2),
            _FakeResource("soil_moisture", device=1),
        ]
    )
    fake = _FakePymodbusClient(silent_devices={2})
    probe = _probe(fake, resources)

    data = probe.read(resources)

    row = data.iloc[0]
    assert row["weather_temp"] == ChannelState.UNKNOWN_ERROR
    assert row["weather_wind"] == ChannelState.UNKNOWN_ERROR
    assert row["soil_moisture"] == 42
    assert 1 in fake.requested_devices


def test_all_devices_answering_read_all_values():
    resources = Resources(
        [
            _FakeResource("a", device=1),
            _FakeResource("b", device=2),
        ]
    )
    probe = _probe(_FakePymodbusClient(), resources)

    row = probe.read(resources).iloc[0]
    assert row["a"] == 42
    assert row["b"] == 42


def test_transport_error_still_raises_connection_error():
    resources = Resources([_FakeResource("a", device=1)])
    probe = _probe(_FakePymodbusClient(transport_error=True), resources)

    with pytest.raises(ConnectionError):
        probe.read(resources)


def test_device_id_is_accepted_as_alias_for_device():
    resources = Resources(
        [
            _FakeResource("a", device_id=5),
            _FakeResource("b", device=1, device_id=9),
            _FakeResource("c"),
        ]
    )
    fake = _FakePymodbusClient()
    probe = _probe(fake, resources)

    row = probe.read(resources).iloc[0]
    assert row["a"] == 42 and row["b"] == 42 and row["c"] == 42
    # a -> alias 5; b -> device wins over device_id; c -> default 1
    assert sorted(fake.requested_devices) == [1, 1, 5]


def test_partial_device_reads_survive_a_mid_scan_silence():
    class _FlakyClient(_FakePymodbusClient):
        def read_holding_registers(self, address, count, device_id):
            self.requested_devices.append(device_id)
            if len(self.requested_devices) > 1 and device_id == 2:
                raise ModbusIOException("went silent mid-scan")
            return _FakeResult()

    resources = Resources(
        [
            _FakeResource("first_ok", device=2),
            _FakeResource("then_silent", device=2),
            _FakeResource("other_device", device=1),
        ]
    )
    probe = _probe(_FlakyClient(), resources)

    row = probe.read(resources).iloc[0]
    assert row["first_ok"] == 42  # the value read before the silence is kept
    assert row["then_silent"] == ChannelState.UNKNOWN_ERROR
    assert row["other_device"] == 42
