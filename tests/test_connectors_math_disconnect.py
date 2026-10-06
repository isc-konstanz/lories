# -*- coding: utf-8 -*-
"""
tests.test_connectors_math_disconnect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A math connector removes the listeners its connect registered when it disconnects, so a
reconnect registers each listening expression once, with the new expression.
"""

from __future__ import annotations

import logging
import os
import threading
from textwrap import dedent

import pytest

import pandas as pd
from lories.components import Component, register_component_type

_SETTINGS_CONF = 'name = "mathdisconnect"\naction = "run"\n\n[interface]\nenabled = false\n'
_SYSTEM_CONF = 'key = "sys"\nname = "Math Disconnect System"\n\n[connectors.math]\ntype = "math"\n'

_CHANNELS = """
    [data.channels.east1]
    type = "float"

    [data.channels.east2]
    type = "float"

    [data.channels.east]
    type = "float"
    connector = "math"
    expression = "east1 + east2"
    listener = true

    [data.channels.west]
    type = "float"
    connector = "math"
    expression = "east1 - east2"
    freq = "1s"
"""


@register_component_type("mathdisconnect")
class MathDisconnectDevice(Component):
    pass


class _Warnings(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.records = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _on_update(data: pd.DataFrame) -> None:
    pass


def _on_other(data: pd.DataFrame) -> None:
    pass


def _load(tmp_path):
    import lories

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir(exist_ok=True)
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "system.conf").write_text(_SYSTEM_CONF)
    (conf_dir / "mathdisconnect.conf").write_text(
        'type = "mathdisconnect"\nname = "Math Disconnect"\n\n' + dedent(_CHANNELS)
    )

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        app = lories.load("mathdisconnect")
    finally:
        os.chdir(cwd)
    (device,) = [c for c in app.components.values() if isinstance(c, MathDisconnectDevice)]
    (connector,) = [c for c in app.connectors.values() if type(c).__name__ == "MathConnector"]
    return app, device, connector


def _connect(app, connector):
    connector.connect(app.channels.filter(lambda c: c.has_connector(connector.id)))


def _listeners_of(app, channel):
    return [listener for listener in app.listeners.values() if listener.id.startswith(f"{channel.id}.")]


def _fire(app, *channels) -> int:
    fired = 0
    for listener in app.listeners.notify(*channels):
        listener(pd.Timestamp.now(tz="UTC"))
        fired += 1
    return fired


def test_disconnect_removes_the_listeners_connect_registered(tmp_path):
    app, device, connector = _load(tmp_path)
    _connect(app, connector)
    east = device.data["east"]
    assert len(_listeners_of(app, east)) == 1

    connector.disconnect()

    assert _listeners_of(app, east) == []
    assert connector._exprs == {}
    assert not connector._connected


def test_reconnect_registers_each_listener_once_with_the_new_expression(tmp_path):
    app, device, connector = _load(tmp_path)
    east1, east2, east = device.data["east1"], device.data["east2"], device.data["east"]
    _connect(app, connector)
    stale = connector._exprs[east.id]

    connector.disconnect()
    _connect(app, connector)

    (listener,) = _listeners_of(app, east)
    assert listener._function is connector._exprs[east.id]
    assert listener._function is not stale
    assert sorted(listener.channels.ids) == sorted([east1.id, east2.id])

    now = pd.Timestamp.now(tz="UTC").floor("s")
    east1.set(now, 1.0)
    east2.set(now, 2.0)
    assert _fire(app, east1) == 1
    assert east.value == pytest.approx(3.0)


def test_shutdown_through_the_connector_context_closes_the_math_connector(tmp_path):
    app, device, connector = _load(tmp_path)
    _connect(app, connector)

    logger = app.connectors._logger
    assert logger.isEnabledFor(logging.WARNING)
    handler = _Warnings()
    logger.addHandler(handler)
    try:
        app.connectors.disconnect()
    finally:
        logger.removeHandler(handler)

    assert [r.getMessage() for r in handler.records if "Failed closing connector" in r.getMessage()] == []
    assert not connector._connected
    assert _listeners_of(app, device.data["east"]) == []


def test_unregister_removes_a_registered_listener_and_ignores_unknown_ones(tmp_path):
    app, device, connector = _load(tmp_path)
    listeners = app.listeners
    listener_id = f"{_on_update.__module__}._on_update"
    count = len(listeners)

    listeners.register(_on_update, app.channels.filter(lambda c: c.key == "east1"))
    assert listener_id in listeners

    listeners.unregister(_on_other)
    assert listener_id in listeners
    assert len(listeners) == count + 1

    listeners.unregister(_on_update)
    assert listener_id not in listeners
    assert len(listeners) == count

    listeners.unregister(_on_update)
    assert len(listeners) == count


def test_unregister_waits_for_the_listener_lock(tmp_path):
    app, device, connector = _load(tmp_path)
    listeners = app.listeners
    listener_id = f"{_on_update.__module__}._on_update"
    listeners.register(_on_update, app.channels.filter(lambda c: c.key == "east1"))

    worker = threading.Thread(target=listeners.unregister, args=(_on_update,))
    with listeners:
        worker.start()
        worker.join(timeout=0.2)
        assert worker.is_alive()
        assert listener_id in listeners

    worker.join(timeout=5)
    assert not worker.is_alive()
    assert listener_id not in listeners
