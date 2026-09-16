# -*- coding: utf-8 -*-
"""
tests.test_connectors_math_isolation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One math channel whose expression cannot be built (a missing input, a bad ``update_on``) must
not take the whole connector down. Before this was pinned, ``MathConnector.connect`` raised on
the first broken expression, the connector's first connect failed, and every math channel that
happened to be processed after the broken one stayed ``DISABLED`` for the life of the process.
"""

from __future__ import annotations

import os
from textwrap import dedent

import pytest

import pandas as pd
from lories._core._channel import ChannelState  # noqa
from lories.components import Component, register_component_type
from lories.connectors.tasks.connect import ConnectTask
from lories.core import ConfigurationError

_SETTINGS_CONF = 'name = "mathisolation"\naction = "run"\n\n[interface]\nenabled = false\n'
_SYSTEM_CONF = 'key = "sys"\nname = "Math Isolation System"\n\n[connectors.math]\ntype = "math"\n'


@register_component_type("mathisolation")
class MathIsolationDevice(Component):
    pass


def _load(tmp_path, channels: str):
    import lories

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir(exist_ok=True)
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "system.conf").write_text(_SYSTEM_CONF)
    component_conf = 'type = "mathisolation"\nname = "Math Isolation"\n\n' + dedent(channels)
    (conf_dir / "mathisolation.conf").write_text(component_conf)

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        app = lories.load("mathisolation")
    finally:
        os.chdir(cwd)
    (device,) = [c for c in app.components.values() if isinstance(c, MathIsolationDevice)]
    (connector,) = [c for c in app.connectors.values() if type(c).__name__ == "MathConnector"]
    return app, device, connector


def _math_channels(app, connector):
    return app.channels.filter(lambda c: c.has_connector(connector.id))


def _fire(app, *channels) -> int:
    fired = 0
    for listener in app.listeners.notify(*channels):
        listener(pd.Timestamp.now(tz="UTC"))
        fired += 1
    return fired


_MIXED = """
    [data.channels.east1]
    type = "float"

    [data.channels.east2]
    type = "float"

    [data.channels.east]
    type = "float"
    connector = "math"
    expression = "east1 + east2"
    listener = true

    [data.channels.broken]
    type = "float"
    connector = "math"
    expression = "east1 + missing"
    listener = true
"""

_ALL_BROKEN = """
    [data.channels.east1]
    type = "float"

    [data.channels.broken1]
    type = "float"
    connector = "math"
    expression = "east1 + missing"

    [data.channels.broken2]
    type = "float"
    connector = "math"
    expression = "east1 * 2"
    update_on = "sometimes"
"""


def test_one_broken_expression_does_not_block_the_others(tmp_path):
    app, device, connector = _load(tmp_path, _MIXED)
    ConnectTask(connector, _math_channels(app, connector))()

    assert device.data["east"].state == ChannelState.CONNECTED
    assert device.data["broken"].state == ChannelState.ARGUMENT_SYNTAX_ERROR

    now = pd.Timestamp.now(tz="UTC").floor("s")
    device.data["east1"].set(now, 1.5)
    device.data["east2"].set(now, 2.0)

    assert _fire(app, device.data["east1"], device.data["east2"]) == 1
    assert device.data["east"].value == pytest.approx(3.5)
    assert not device.data["broken"].is_valid()
    assert device.data["broken"].state == ChannelState.ARGUMENT_SYNTAX_ERROR


def test_all_expressions_broken_is_a_configuration_error(tmp_path):
    app, device, connector = _load(tmp_path, _ALL_BROKEN)
    with pytest.raises(ConfigurationError, match="No valid math expression"):
        connector.connect(_math_channels(app, connector))


def test_read_marks_the_broken_channel_and_evaluates_the_rest(tmp_path):
    app, device, connector = _load(tmp_path, _MIXED.replace("listener = true\n", ""))
    channels = _math_channels(app, connector)
    connector.connect(channels)

    now = pd.Timestamp.now(tz="UTC").floor("s")
    device.data["east1"].set(now, 1.0)
    device.data["east2"].set(now, 2.0)

    frame = connector.read(channels)
    assert frame.iloc[-1][device.data["east"].id] == pytest.approx(3.0)
    assert frame.iloc[-1][device.data["broken"].id] == ChannelState.ARGUMENT_SYNTAX_ERROR
