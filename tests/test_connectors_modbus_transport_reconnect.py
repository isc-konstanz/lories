# -*- coding: utf-8 -*-
"""
lories.tests.test_connectors_modbus_transport_reconnect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

pymodbus' sync TCP client raises the raw socket error (``BrokenPipeError`` on
Linux, ``ConnectionResetError`` on Windows) when the peer closed the connection,
and keeps the dead socket, so ``client.connected`` stays True. The connector used
to map that ``IOError`` to a generic ``ConnectorError``: the task never tore the
connector down, ``_is_reconnectable()`` stayed False and every interval repeated
``Failed reading connector '...': [Errno 32] Broken pipe`` until a restart
(lories-frictions issue 06, ISC box outage 2026-09-25).

A transport ``IOError`` now raises ``ConnectionError`` from read and write, so
``ConnectorTask`` disconnects the connector and the main loop reconnects after
``_interval_reconnect``.
"""

from __future__ import annotations

import logging
from threading import Lock
from types import SimpleNamespace

import pytest

import pandas as pd

pytest.importorskip("pymodbus")

from lories._core._connector import ConnectType  # noqa: E402
from lories.connectors.connector import Connector  # noqa: E402
from lories.connectors.errors import ConnectionError, ConnectorError  # noqa: E402
from lories.connectors.modbus.client import ModbusClient  # noqa: E402
from lories.connectors.tasks import ReadTask, WriteTask  # noqa: E402
from lories.core.resources import Resources  # noqa: E402


class _FakeResource:
    def __init__(self, id, address=0, **configs):
        self.id = id
        self.address = address
        self._configs = configs

    def get(self, key, default=None):
        return self._configs.get(key, default)

    @staticmethod
    def has_connector(_id) -> bool:
        # No channel state to stamp: set_channels skips it on the way out.
        return False


class _FakeChannels(list):
    """What WriteTask needs from a channel set: a frame of the values to write."""

    def to_frame(self, unique: bool = True) -> pd.DataFrame:
        return pd.DataFrame({c.id: [1] for c in self}, index=[pd.Timestamp.now(tz="UTC")])


class _DeadSocketClient:
    """The pymodbus 3.9 sync client after the peer reset the connection."""

    def __init__(self, error=BrokenPipeError(32, "Broken pipe")):
        self.error = error
        self.connected = True

    def read_holding_registers(self, address, *, count=1, slave=1):
        raise self.error

    @staticmethod
    def convert_to_registers(value, type, word_order):
        return [int(value)]

    def write_registers(self, address, values, slave=1):
        raise self.error

    def close(self):
        self.connected = False


class _ProbeModbusClient(ModbusClient):
    id = "modbus_probe"

    def is_enabled(self):
        return True

    def is_configured(self):
        return True


def _probe(fake_client, resources, wrapped: bool = False) -> ModbusClient:
    probe = _ProbeModbusClient.__new__(_ProbeModbusClient)
    probe._endian = "big"
    probe._ModbusClient__client = fake_client
    probe._ModbusClient__registers = {
        r.id: SimpleNamespace(address=r.address, length=1, function="holding_register", type=None) for r in resources
    }
    probe._logger = logging.getLogger("test.modbus_probe")
    if wrapped:
        # What ConnectorMeta.__call__ does for a constructed connector: route the
        # public methods through the _do_* wrappers that own the lifecycle flags.
        probe._Connector__resources = resources
        probe._lock = Lock()
        probe._connected = True
        probe._connect_type = ConnectType.AUTO
        probe._timestamp_connect = pd.Timestamp.now(tz="UTC")
        probe._timestamp_disconnect = pd.NaT
        probe._interval_reconnect = pd.Timedelta(0)
        for method in ("connect", "disconnect", "read", "write"):
            Connector._wrap_method(probe, method)
    return probe


@pytest.mark.parametrize("error", [BrokenPipeError(32, "Broken pipe"), ConnectionResetError(104, "reset")])
def test_transport_oserror_on_read_raises_connection_error(error):
    resources = Resources([_FakeResource("a", device=1)])
    probe = _probe(_DeadSocketClient(error), resources)

    with pytest.raises(ConnectionError, match="Connection lost"):
        probe.read(resources)


def test_transport_oserror_on_write_raises_connection_error():
    resources = Resources([_FakeResource("a", device=1)])
    probe = _probe(_DeadSocketClient(), resources)
    probe._test_channels = resources
    type(probe).channels = property(lambda self: self._test_channels)
    try:
        with pytest.raises(ConnectionError, match="Connection lost"):
            probe.write(pd.DataFrame({"a": [1]}, index=[pd.Timestamp.now(tz="UTC")]))
    finally:
        del type(probe).channels


def test_read_task_disconnects_and_connector_becomes_reconnectable():
    resources = Resources([_FakeResource("a", device=1)])
    client = _DeadSocketClient()
    probe = _probe(client, resources, wrapped=True)
    assert probe._is_connected()
    assert not probe._is_reconnectable()

    with pytest.raises(ConnectorError):
        ReadTask(probe, resources)()

    assert client.connected is False, "the dead pymodbus socket must be closed"
    assert probe._connected is False
    assert not probe._is_connected()
    assert probe._is_reconnectable()


def test_write_task_disconnects_on_transport_error():
    resources = Resources([_FakeResource("a", device=1)])
    client = _DeadSocketClient()
    probe = _probe(client, resources, wrapped=True)
    probe._test_channels = resources
    type(probe).channels = property(lambda self: self._test_channels)
    try:
        with pytest.raises(ConnectorError):
            WriteTask(probe, _FakeChannels(resources))()
    finally:
        del type(probe).channels

    assert client.connected is False
    assert probe._connected is False
