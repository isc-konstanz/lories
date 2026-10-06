# -*- coding: utf-8 -*-
"""
tests.test_channels_connector_vars
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A channel's connector wrapper exposes the extra configs of its connector table.
"""

from __future__ import annotations

import os
from textwrap import dedent

from lories.components import Component, register_component_type

_SETTINGS_CONF = 'name = "channelvars"\naction = "run"\n\n[interface]\nenabled = false\n'
_SYSTEM_CONF = 'key = "sys"\nname = "Channel Vars System"\n\n[connectors.math]\ntype = "math"\n'

_CHANNELS = """
    [data.channels.reading]
    type = "float"

    [data.channels.reading.connector]
    connector = "math"
    address = 40001
"""


@register_component_type("channelvars")
class ChannelVarsDevice(Component):
    pass


def _load(tmp_path):
    import lories

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir(exist_ok=True)
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "system.conf").write_text(_SYSTEM_CONF)
    (conf_dir / "channelvars.conf").write_text('type = "channelvars"\nname = "Channel Vars"\n\n' + dedent(_CHANNELS))

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        app = lories.load("channelvars")
    finally:
        os.chdir(cwd)
    (device,) = [c for c in app.components.values() if isinstance(c, ChannelVarsDevice)]
    (connector,) = [c for c in app.connectors.values() if type(c).__name__ == "MathConnector"]
    return app, device, connector


def test_channel_connector_get_reads_its_configs(tmp_path):
    app, device, connector = _load(tmp_path)
    channel_connector = device.data["reading"].connector
    assert channel_connector.id == connector.id

    assert channel_connector.get("address") == 40001
    assert channel_connector.get("missing", "fallback") == "fallback"


def test_channel_connector_getitem_reads_its_configs(tmp_path):
    app, device, connector = _load(tmp_path)

    assert device.data["reading"].connector["address"] == 40001


def test_channel_connector_str_lists_its_configs(tmp_path):
    app, device, connector = _load(tmp_path)

    text = str(device.data["reading"].connector)
    assert f"id={connector.id}" in text
    assert "address=40001" in text
