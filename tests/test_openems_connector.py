# -*- coding: utf-8 -*-
"""
tests.test_openems_connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Protocol-level unit tests for the OpenEMS WebSocket connector wire format
(``lories.connectors.openems``). No socket / network use:
``websocket.WebSocketApp`` (as imported by the connector module) is replaced
with a fake that records outbound ``send()`` payloads and, when scripted,
feeds a canned reply straight back into the connector's own ``on_message``
callback before ``send()`` returns. ``connect()``'s background thread then
runs its fake ``run_forever`` to completion synchronously, so the whole
handshake is deterministic and no test sleeps beyond the (small, configured)
auth timeout.

Connectors are built via ``__new__`` (bypassing ``ConnectorMeta.__call__``'s
method wrapping and the full application-context ``__init__``), the same
pattern used in ``test_connectors_sql_database.py``: the raw connector logic
is driven directly against hand-set attributes.
"""

from __future__ import annotations

import json
import logging
import time
from threading import Event

import pytest

import lories.connectors.openems as openems_connector
import pandas as pd
from lories.connectors.errors import ConnectionError, ConnectorError
from lories.connectors.openems import OpenEMSBackendConnector, OpenEMSEdgeConnector
from lories.io.jsonrpc import JsonRpc

AUTH_SUCCESS = json.dumps({"jsonrpc": "2.0", "id": "srv-1", "result": {"token": "abcdef1234"}})


def _auth_error_frame(message: str = "invalid username or password") -> str:
    return json.dumps({"jsonrpc": "2.0", "id": "srv-1", "error": {"message": message}})


# ---------------------------------------------------------------- fakes


class FakeWebSocketApp:
    """Stand-in for ``websocket.WebSocketApp``.

    ``send`` records the outgoing payload and, if a scripted response is
    queued, feeds it straight back into ``on_message`` before returning.
    """

    def __init__(self, url, on_open=None, on_message=None, on_error=None, on_close=None, responses=None):
        self.url = url
        self.on_open = on_open
        self.on_message = on_message
        self.on_error = on_error
        self.on_close = on_close
        self.sent = []
        self.closed = False
        self._responses = list(responses) if responses else []

    def send(self, payload: str) -> None:
        self.sent.append(payload)
        if self._responses:
            response = self._responses.pop(0)
            if response is not None:
                self.on_message(self, response)

    def run_forever(self, ping_interval=None, ping_timeout=None) -> None:
        if self.on_open is not None:
            self.on_open(self)

    def close(self) -> None:
        self.closed = True


def _install_fake_ws_app(monkeypatch, responses=None):
    """Patch ``websocket.WebSocketApp`` and return the list of fakes it will build."""

    created = []

    def _factory(url, on_open=None, on_message=None, on_error=None, on_close=None, **_kwargs):
        fake = FakeWebSocketApp(
            url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close, responses=responses
        )
        created.append(fake)
        return fake

    monkeypatch.setattr(openems_connector.websocket, "WebSocketApp", _factory)
    return created


class FakeChannel:
    """Records ``(timestamp, value)`` pairs handed to it, like a real ``Channel.set``."""

    def __init__(self, address: str):
        self.address = address
        self.calls = []

    def set(self, timestamp, value) -> None:
        self.calls.append((timestamp, value))


# ---------------------------------------------------------------- construction helper


def _make_connector(cls, *, timeout: int = 10, edge_id: str = None):
    connector = cls.__new__(cls)
    connector._id = "test.openems"
    connector._logger = logging.getLogger("test_openems_connector")
    connector._host = "openems.example"
    connector._ws_port = 8085
    connector._username = "admin"
    connector._password = "admin"
    connector._timeout = timeout
    connector._edge_id = edge_id if edge_id is not None else ("0" if cls is OpenEMSEdgeConnector else "edge0")
    connector._ws_app = None
    connector._ws_thread = None
    connector._connected_event = Event()
    connector._auth_error = None
    connector._listeners = {}
    connector._subscribe_count = 0
    connector._json_rpc = JsonRpc()
    return connector


def _connect(monkeypatch, cls, *, responses, timeout=10, channels=None):
    channels = channels if channels is not None else [FakeChannel("Comp/Chan")]
    created = _install_fake_ws_app(monkeypatch, responses=responses)
    connector = _make_connector(cls, timeout=timeout)
    connector.connect(channels)
    # connect() unblocks on the connected event, which the WS thread sets BEFORE
    # sending the subscribe request; join the (synchronous) fake thread so every
    # scripted send has landed before the test asserts on fake.sent.
    if connector._ws_thread is not None:
        connector._ws_thread.join(timeout=5)
    return connector, created[0]


# ---------------------------------------------------------------- 1 + 2: Edge auth / subscribe payload


def test_edge_auth_success_triggers_subscribe_with_expected_payload(monkeypatch):
    channel = FakeChannel("Comp/Chan1")
    connector, fake = _connect(monkeypatch, OpenEMSEdgeConnector, responses=[AUTH_SUCCESS], channels=[channel])

    assert connector.is_connected() is True
    assert len(fake.sent) == 2

    auth_request = json.loads(fake.sent[0])
    assert auth_request["method"] == "authenticateWithPassword"
    assert auth_request["params"] == {"username": "admin", "password": "admin"}

    subscribe_request = json.loads(fake.sent[1])
    assert subscribe_request["method"] == "edgeRpc"
    assert subscribe_request["params"]["edgeId"] == "0"
    inner = subscribe_request["params"]["payload"]
    assert inner["method"] == "subscribeChannels"
    assert inner["params"] == {"count": 0, "channels": ["Comp/Chan1"]}


def test_edge_subscribe_count_increments_across_successive_subscribes(monkeypatch):
    channel = FakeChannel("Comp/Chan1")
    connector, fake = _connect(monkeypatch, OpenEMSEdgeConnector, responses=[AUTH_SUCCESS], channels=[channel])

    first_count = json.loads(fake.sent[1])["params"]["payload"]["params"]["count"]
    assert first_count == 0
    assert connector._subscribe_count == 1

    connector._send_subscribe(fake)

    second_count = json.loads(fake.sent[2])["params"]["payload"]["params"]["count"]
    assert second_count == 1
    assert connector._subscribe_count == 2


# ---------------------------------------------------------------- 3: Backend subscribe payload


def test_backend_auth_success_triggers_subscribe_with_expected_payload(monkeypatch):
    channel = FakeChannel("Comp/Chan1")
    connector, fake = _connect(monkeypatch, OpenEMSBackendConnector, responses=[AUTH_SUCCESS], channels=[channel])

    assert connector.is_connected() is True
    subscribe_request = json.loads(fake.sent[1])
    assert subscribe_request["method"] == "subscribeEdgesChannels"
    assert subscribe_request["params"] == {"count": 0, "ids": ["edge0"], "channels": ["Comp/Chan1"]}


# ---------------------------------------------------------------- 4: Edge dispatch


def test_edge_currentData_routes_known_ignores_unknown_drops_none(monkeypatch):
    chan_a = FakeChannel("Comp/ChanA")
    chan_b = FakeChannel("Comp/ChanB")
    _, fake = _connect(monkeypatch, OpenEMSEdgeConnector, responses=[AUTH_SUCCESS], channels=[chan_a, chan_b])

    notification = json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "edgeRpc",
            "params": {
                "edgeId": "0",
                "payload": {
                    "method": "currentData",
                    "params": {"Comp/ChanA": 42, "Comp/ChanB": None, "Comp/Unknown": 7},
                },
            },
        }
    )
    fake.on_message(fake, notification)

    assert len(chan_a.calls) == 1
    timestamp, value = chan_a.calls[0]
    assert value == 42
    assert isinstance(timestamp, pd.Timestamp) and timestamp.tzinfo is not None
    assert chan_b.calls == []  # None value dropped, not forwarded


# ---------------------------------------------------------------- 5: Backend dispatch


def test_backend_edgesCurrentData_routes_and_ignores_non_dict_entries(monkeypatch):
    chan_a = FakeChannel("Comp/ChanA")
    _, fake = _connect(monkeypatch, OpenEMSBackendConnector, responses=[AUTH_SUCCESS], channels=[chan_a])

    notification = json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "edgesCurrentData",
            "params": {
                "edge0": {"Comp/ChanA": 5, "Comp/Unknown": 9},
                "malformed": "not-a-dict",
            },
        }
    )
    fake.on_message(fake, notification)  # the non-dict "malformed" entry must not raise

    assert len(chan_a.calls) == 1
    timestamp, value = chan_a.calls[0]
    assert value == 5
    assert isinstance(timestamp, pd.Timestamp) and timestamp.tzinfo is not None


# ---------------------------------------------------------------- 6: malformed frames never raise


def test_on_message_never_raises_on_malformed_frames(monkeypatch):
    chan = FakeChannel("Comp/ChanA")
    _, fake = _connect(monkeypatch, OpenEMSEdgeConnector, responses=[AUTH_SUCCESS], channels=[chan])

    fake.on_message(fake, "not json at all {")  # non-JSON string

    error_frame = json.dumps({"jsonrpc": "2.0", "id": "x", "error": {"message": "boom"}})
    fake.on_message(fake, error_frame)  # post-connect JSON-RPC error: logged, not raised

    bad_payload = json.dumps(
        {"jsonrpc": "2.0", "method": "edgeRpc", "params": {"edgeId": "0", "payload": "not-a-dict"}}
    )
    fake.on_message(fake, bad_payload)  # non-dict Edge payload: dispatch bails out quietly

    assert chan.calls == []


# ---------------------------------------------------------------- 7: auth-error fail-fast


def test_auth_error_frame_fails_connect_without_waiting_for_timeout(monkeypatch):
    channel = FakeChannel("Comp/Chan1")
    _install_fake_ws_app(monkeypatch, responses=[_auth_error_frame("invalid username or password")])
    connector = _make_connector(OpenEMSEdgeConnector, timeout=10)

    start = time.monotonic()
    with pytest.raises(ConnectionError, match="invalid username or password"):
        connector.connect([channel])
    elapsed = time.monotonic() - start

    assert elapsed < 2.0  # nowhere near the configured 10s timeout


# ---------------------------------------------------------------- 8: timeout path


def test_connect_raises_connection_error_on_auth_timeout(monkeypatch):
    channel = FakeChannel("Comp/Chan1")
    _install_fake_ws_app(monkeypatch, responses=[None])  # server never replies
    connector = _make_connector(OpenEMSEdgeConnector, timeout=1)

    with pytest.raises(ConnectionError, match="Timeout"):
        connector.connect([channel])


# ---------------------------------------------------------------- 9: read()/write() raise ConnectorError


def test_read_raises_connector_error():
    connector = _make_connector(OpenEMSEdgeConnector)
    with pytest.raises(ConnectorError):
        connector.read([])


def test_write_raises_connector_error():
    connector = _make_connector(OpenEMSEdgeConnector)
    with pytest.raises(ConnectorError):
        connector.write(pd.DataFrame())


# ---------------------------------------------------------------- 10: disconnect


def test_edge_disconnect_sends_unsubscribe_and_is_idempotent(monkeypatch):
    channel = FakeChannel("Comp/Chan1")
    connector, fake = _connect(monkeypatch, OpenEMSEdgeConnector, responses=[AUTH_SUCCESS], channels=[channel])

    connector.disconnect()

    unsubscribe = json.loads(fake.sent[-1])
    assert unsubscribe["method"] == "edgeRpc"
    inner = unsubscribe["params"]["payload"]
    assert inner["method"] == "subscribeChannels"
    assert inner["params"]["channels"] == []

    assert connector.is_connected() is False
    assert connector._ws_app is None
    assert fake.closed is True

    connector.disconnect()  # second call must be a safe no-op


def test_backend_disconnect_sends_unsubscribe_and_is_idempotent(monkeypatch):
    channel = FakeChannel("Comp/Chan1")
    connector, fake = _connect(monkeypatch, OpenEMSBackendConnector, responses=[AUTH_SUCCESS], channels=[channel])

    connector.disconnect()

    unsubscribe = json.loads(fake.sent[-1])
    assert unsubscribe["method"] == "subscribeEdgesChannels"
    assert unsubscribe["params"]["ids"] == []

    assert connector.is_connected() is False
    connector.disconnect()  # second call must be a safe no-op
