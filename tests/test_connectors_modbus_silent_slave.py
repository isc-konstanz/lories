# -*- coding: utf-8 -*-
"""
lories.tests.test_connectors_modbus_silent_slave
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A Modbus slave that does not answer makes pymodbus RAISE ``ModbusIOException``
(``result.isError()`` covers only protocol exception responses). That used to
escape the per-resource handling and abort the whole connector read, blanking
every channel of every device on the connector (lories-frictions issue 05).
Reads now isolate the failure per device group and continue; writes mark the
failed group's channels ``WRITE_ERROR`` (the ``VALID`` state stamped by
``set_frame`` would otherwise stand). Transport errors still raise
``ConnectionError``. ``device_id`` is accepted as an alias for ``device``.

The fake client mirrors the pymodbus 3.9 sync signature (keyword-only
``count``/``slave``, no ``device_id``), so these tests exercise the
``TypeError -> slave=`` fallback -- the call path the shipped library takes.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

import pandas as pd

pytest.importorskip("pymodbus")

from pymodbus.exceptions import ConnectionException, ModbusException, ModbusIOException  # noqa: E402

from lories._core import ChannelState  # noqa: E402
from lories.connectors.errors import ConnectionError  # noqa: E402
from lories.connectors.modbus.client import ModbusClient  # noqa: E402
from lories.core.resources import Resources  # noqa: E402
from lories.data.channels import Channels  # noqa: E402

_BAD_ADDRESS = 99  # the fake's convert_from_registers rejects reads from this address


class _FakeResource:
    def __init__(self, id, address=0, **configs):
        self.id = id
        self.address = address
        self._configs = configs

    def get(self, key, default=None):
        return self._configs.get(key, default)


class _FakeChannel(_FakeResource):
    state = None


class _FakeResult:
    def __init__(self, address=0):
        self.registers = [address if address == _BAD_ADDRESS else 42]

    @staticmethod
    def isError() -> bool:
        return False


class _FakePymodbusClient:
    def __init__(self, silent_devices=(), transport_error=False):
        self.silent_devices = set(silent_devices)
        self.transport_error = transport_error
        self.requested_devices = []
        self.written = []

    def read_holding_registers(self, address, *, count=1, slave=1):
        if self.transport_error:
            raise ConnectionException("transport down")
        self.requested_devices.append(slave)
        if slave in self.silent_devices:
            raise ModbusIOException("no response received")
        return _FakeResult(address)

    @staticmethod
    def convert_from_registers(registers, type, word_order):
        if registers[0] == _BAD_ADDRESS:
            raise ModbusException("Registers illegal size")
        return registers[0]

    @staticmethod
    def convert_to_registers(value, type, word_order):
        return [int(value)]

    def write_registers(self, address, values, slave=1):
        if slave in self.silent_devices:
            raise ModbusIOException("no response received")
        self.written.append((slave, address, tuple(values)))


class _ProbeModbusClient(ModbusClient):
    id = "modbus_probe"
    _test_channels = None

    @property
    def channels(self):
        return self._test_channels


def _probe(fake_client, resources) -> ModbusClient:
    probe = _ProbeModbusClient.__new__(_ProbeModbusClient)
    probe._endian = "big"
    probe._ModbusClient__client = fake_client
    probe._ModbusClient__registers = {
        r.id: SimpleNamespace(address=r.address, length=1, function="holding_register", type=None) for r in resources
    }
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
        def read_holding_registers(self, address, *, count=1, slave=1):
            self.requested_devices.append(slave)
            if len(self.requested_devices) > 1 and slave == 2:
                raise ModbusIOException("went silent mid-scan")
            return _FakeResult(address)

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


def test_malformed_response_is_contained_to_the_resource():
    resources = Resources(
        [
            _FakeResource("bad", device=1, address=_BAD_ADDRESS),
            _FakeResource("good", device=1),
        ]
    )
    probe = _probe(_FakePymodbusClient(), resources)

    row = probe.read(resources).iloc[0]
    assert row["bad"] == ChannelState.UNKNOWN_ERROR
    assert row["good"] == 42


def test_silent_device_write_marks_write_error_and_continues():
    channel_b = _FakeChannel("b", device=2)
    channel_c = _FakeChannel("c", device=2)
    channel_a = _FakeChannel("a", device=1)
    channel_skipped = _FakeChannel("skipped", device=2)  # no data column -> untouched
    channels = Channels([channel_b, channel_c, channel_a, channel_skipped])

    fake = _FakePymodbusClient(silent_devices={2})
    probe = _probe(fake, channels)
    probe._test_channels = channels

    timestamp = pd.Timestamp("2026-09-02 12:00:00", tz="UTC")
    data = pd.DataFrame({"a": [1.0], "b": [2.0], "c": [3.0]}, index=[timestamp])

    probe.write(data)

    # The silent group is marked instead of keeping set_frame's VALID state ...
    assert channel_b.state == ChannelState.WRITE_ERROR
    assert channel_c.state == ChannelState.WRITE_ERROR
    # ... the other device is still written, and untouched channels stay untouched
    assert [(slave, values) for slave, _addr, values in fake.written] == [(1, (1,))]
    assert channel_a.state is None
    assert channel_skipped.state is None
