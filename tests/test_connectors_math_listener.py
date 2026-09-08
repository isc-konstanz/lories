# -*- coding: utf-8 -*-
"""
tests.test_connectors_math_listener
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A math channel with ``listener = true`` is excluded from scheduled reads and only ever
receives a value through the listener the connector registers on its inputs. Before this
was pinned, that listener was registered with ``how=None`` (the never-implemented
``listen`` key), so ``Listener.has_update`` was always False and every listening math
channel stayed unwritten forever. The mode now comes from ``update_on`` ("any" | "all").
"""

from __future__ import annotations

import os
from textwrap import dedent

import pytest

import pandas as pd
from lories.components import Component, register_component_type
from lories.core import ConfigurationError

_SETTINGS_CONF = 'name = "mathtest"\naction = "run"\n\n[interface]\nenabled = false\n'
_SYSTEM_CONF = 'key = "sys"\nname = "Math Test System"\n\n[connectors.math]\ntype = "math"\n'


@register_component_type("mathtest")
class MathTestDevice(Component):
    pass


def _load(tmp_path, channels: str):
    import lories

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir(exist_ok=True)
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "system.conf").write_text(_SYSTEM_CONF)
    (conf_dir / "mathtest.conf").write_text('type = "mathtest"\nname = "Math Test"\n\n' + dedent(channels))

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        app = lories.load("mathtest")
    finally:
        os.chdir(cwd)
    (device,) = [c for c in app.components.values() if isinstance(c, MathTestDevice)]
    (connector,) = [c for c in app.connectors.values() if type(c).__name__ == "MathConnector"]
    return app, device, connector


def _connect(app, connector):
    connector.connect(app.channels.filter(lambda c: c.has_connector(connector.id)))


def _fire(app, *channels) -> int:
    """Dispatch every listener that has an update, the way the application tick does."""
    fired = 0
    for listener in app.listeners.notify(*channels):
        listener(pd.Timestamp.now(tz="UTC"))
        fired += 1
    return fired


_SUM = """
    [data.channels.east1]
    type = "float"

    [data.channels.east2]
    type = "float"

    [data.channels.east]
    type = "float"
    connector = "math"
    expression = "east1 + east2"
    freq = "1s"
    listener = true
    {extra}
"""


def test_listener_true_writes_on_input_update(tmp_path):
    app, device, connector = _load(tmp_path, _SUM.format(extra=""))
    _connect(app, connector)

    now = pd.Timestamp.now(tz="UTC").floor("s")
    device.data["east1"].set(now, 1.5)
    device.data["east2"].set(now, 2.0)

    assert _fire(app, device.data["east1"], device.data["east2"]) == 1
    assert device.data["east"].is_valid()
    assert device.data["east"].value == pytest.approx(3.5)


def test_update_on_any_fires_on_a_single_input(tmp_path):
    app, device, connector = _load(tmp_path, _SUM.format(extra='update_on = "any"'))
    _connect(app, connector)

    now = pd.Timestamp.now(tz="UTC").floor("s")
    device.data["east1"].set(now, 1.0)
    device.data["east2"].set(now, 2.0)
    assert _fire(app, device.data["east1"], device.data["east2"]) == 1
    assert device.data["east"].value == pytest.approx(3.0)

    later = now + pd.Timedelta(seconds=1)
    device.data["east1"].set(later, 5.0)
    assert _fire(app, device.data["east1"]) == 1
    assert device.data["east"].value == pytest.approx(7.0)


def test_update_on_all_waits_for_every_input(tmp_path):
    app, device, connector = _load(tmp_path, _SUM.format(extra='update_on = "all"'))
    _connect(app, connector)

    now = pd.Timestamp.now(tz="UTC").floor("s")
    device.data["east1"].set(now, 1.0)
    assert _fire(app, device.data["east1"]) == 0
    assert not device.data["east"].is_valid()

    device.data["east2"].set(now, 2.0)
    assert _fire(app, device.data["east2"]) == 1
    assert device.data["east"].value == pytest.approx(3.0)


def test_update_on_implies_listener(tmp_path):
    channels = _SUM.replace("listener = true\n", "").format(extra='update_on = "all"')
    app, device, connector = _load(tmp_path, channels)
    _connect(app, connector)

    now = pd.Timestamp.now(tz="UTC").floor("s")
    device.data["east1"].set(now, 1.0)
    device.data["east2"].set(now, 2.0)
    assert _fire(app, device.data["east1"], device.data["east2"]) == 1
    assert device.data["east"].value == pytest.approx(3.0)


def test_unknown_update_on_is_a_configuration_error(tmp_path):
    app, device, connector = _load(tmp_path, _SUM.format(extra='update_on = "sometimes"'))
    with pytest.raises(ConfigurationError, match="update_on"):
        _connect(app, connector)


def test_without_listener_the_read_path_still_evaluates(tmp_path):
    app, device, connector = _load(tmp_path, _SUM.replace("listener = true\n", "").format(extra=""))
    _connect(app, connector)

    now = pd.Timestamp.now(tz="UTC").floor("s")
    device.data["east1"].set(now, 1.0)
    device.data["east2"].set(now, 2.0)
    assert _fire(app, device.data["east1"], device.data["east2"]) == 0

    frame = connector.read(app.channels.filter(lambda c: c.has_connector(connector.id)))
    assert frame.iloc[-1][device.data["east"].id] == pytest.approx(3.0)
