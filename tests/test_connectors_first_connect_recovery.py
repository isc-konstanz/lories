# -*- coding: utf-8 -*-
"""
tests.test_connectors_first_connect_recovery
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A connector whose connect attempt fails keeps the channels that attempt was given,
so the main loop's reconnect retries with the same set and the channels show the
attempt's state instead of staying ``DISABLED``.
"""

from __future__ import annotations

import os
from textwrap import dedent

import pytest

import pandas as pd
from lories.components import Component, register_component_type
from lories.connectors import ConnectionError, Connector, register_connector_type
from lories.data.channels import Channel, ChannelState

_SETTINGS_CONF = 'name = "firstconnect"\naction = "run"\n\n[interface]\nenabled = false\n'
_SYSTEM_CONF = 'key = "sys"\nname = "First Connect System"\n\n[connectors.flaky]\ntype = "flaky_probe"\n'
_CHANNELS = """
    [data.channels.a]
    type = "float"
    connector = "flaky"

    [data.channels.b]
    type = "float"
    connector = "flaky"

    [data.channels.local]
    type = "float"
"""


@register_component_type("firstconnect")
class FirstConnectDevice(Component):
    pass


@register_connector_type("flaky_probe", replace=True)
class FlakyConnector(Connector):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.failures = []
        self.attempts = []

    def connect(self, resources) -> None:
        self.attempts.append(
            {
                "ids": sorted(r.id for r in resources),
                "states": {c.id: c.state for c in resources if isinstance(c, Channel)},
            }
        )
        if len(self.failures) > 0:
            raise self.failures.pop(0)

    def read(self, resources) -> pd.DataFrame:
        return pd.DataFrame()

    def write(self, data: pd.DataFrame) -> None:
        pass


@pytest.fixture
def loaded(tmp_path):
    import lories

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "system.conf").write_text(_SYSTEM_CONF)
    (conf_dir / "firstconnect.conf").write_text('type = "firstconnect"\nname = "First Connect"\n\n' + dedent(_CHANNELS))

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        app = lories.load("firstconnect")
    finally:
        os.chdir(cwd)
    (connector,) = [c for c in app.connectors.values() if isinstance(c, FlakyConnector)]
    connector._interval_reconnect = pd.Timedelta(0)
    try:
        yield app, connector
    finally:
        app.interrupt()


def _channels(app, connector):
    return app.channels.filter(lambda c: c.has_connector(connector.id))


def _states(app, connector):
    return {c.id: c.state for c in _channels(app, connector)}


def _local_states(app, connector):
    return {c.id: c.state for c in app.channels.filter(lambda c: not c.has_connector(connector.id))}


def _reconnect(app, connector):
    assert connector._is_reconnectable()
    app.connectors.reconnect(lambda c: c._is_reconnectable())
    future = app.connectors._ConnectorContext__reconnect_futures[connector.id]
    future.exception(timeout=10)


def test_connection_error_on_first_connect_recovers_with_the_same_channels(loaded):
    app, connector = loaded
    connector.failures = [ConnectionError(connector, "peer down")]
    expected = sorted(_channels(app, connector).ids)
    assert len(expected) == 2
    local_before = _local_states(app, connector)

    app.connectors.connect(channels=app.channels)

    assert connector.attempts[0]["ids"] == expected
    assert not connector._is_connected()
    assert pd.isna(connector._timestamp_connect)
    assert not pd.isna(connector._timestamp_disconnect)
    assert sorted(connector.channels.ids) == expected
    assert set(_states(app, connector).values()) == {ChannelState.DISCONNECTED}

    _reconnect(app, connector)

    assert len(connector.attempts) == 2
    assert connector.attempts[1]["ids"] == expected
    assert connector._is_connected()
    assert sorted(connector.channels.ids) == expected
    assert set(_states(app, connector).values()) == {ChannelState.CONNECTED}
    assert _local_states(app, connector) == local_before


def test_other_exception_on_first_connect_keeps_channels_and_disconnects_them(loaded):
    app, connector = loaded
    connector.failures = [RuntimeError("boom")]
    expected = sorted(_channels(app, connector).ids)

    app.connectors.connect(channels=app.channels)

    assert not connector._is_connected()
    assert sorted(connector.channels.ids) == expected
    assert set(_states(app, connector).values()) == {ChannelState.DISCONNECTED}

    _reconnect(app, connector)

    assert connector.attempts[1]["ids"] == expected
    assert connector._is_connected()
    assert set(_states(app, connector).values()) == {ChannelState.CONNECTED}


def test_first_attempt_shows_connecting_while_connect_runs(loaded):
    app, connector = loaded
    connector.failures = [RuntimeError("boom")]
    expected = sorted(_channels(app, connector).ids)

    app.connectors.connect(channels=app.channels)

    assert sorted(connector.attempts[0]["states"]) == expected
    assert set(connector.attempts[0]["states"].values()) == {ChannelState.CONNECTING}


def test_successful_first_connect_is_unchanged(loaded):
    app, connector = loaded
    expected = sorted(_channels(app, connector).ids)
    local_before = _local_states(app, connector)

    app.connectors.connect(channels=app.channels)

    assert len(connector.attempts) == 1
    assert connector.attempts[0]["ids"] == expected
    assert connector._is_connected()
    assert not pd.isna(connector._timestamp_connect)
    assert pd.isna(connector._timestamp_disconnect)
    assert not connector._is_reconnectable()
    assert sorted(connector.channels.ids) == expected
    assert set(_states(app, connector).values()) == {ChannelState.CONNECTED}
    assert _local_states(app, connector) == local_before
