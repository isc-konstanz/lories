# -*- coding: utf-8 -*-
"""
lories.tests.test_data_access_channels_file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Channels declared in a component's ``<name>.d/channels.conf`` are registered
by ``DataAccess.load`` from a transient ``Configurations`` object. The
component's own ``configs`` must still learn about them: everything that
renders a configurator's configs (the Dash "Edit configs" modal, ``str()``)
walks ``component.configs`` and would otherwise show fewer channels than the
runtime actually has.
"""

from __future__ import annotations

import json
import sys
from argparse import ArgumentParser

import pytest

from lories.application import Application

_SETTINGS = """
[data]
freq = "1min"
"""

_SYSTEM = """
key = "probe"
name = "Probe System"

[location]
latitude = 47.67
longitude = 9.15
timezone = "Europe/Berlin"

[data.channels]
group = "probe"
type = "float"

[data.channels.inline_ch]
name = "Inline channel (system.conf)"
"""

_SYSTEM_CHANNELS = """
aggregate = "mean"

[logger]
table = "probe"

[dfile_ch]
name = "Channel from system.d/channels.conf"
"""

_MACHINES = """
name = "Machines"
type = "component"

[data.channels]
group = "machines"
type = "float"

[data.channels.inline_m]
name = "Inline machine channel (machines.conf)"

[data.channels.both]
name = "Defined inline"
unit = "W"
"""

_MACHINES_CHANNELS = """
freq = "30s"

[dfile_m]
name = "Machine channel from machines.d/channels.conf"

[both]
name = "Overridden by machines.d/channels.conf"
"""

_NO_INLINE = """
key = "probe"
name = "Probe System"

[location]
latitude = 47.67
longitude = 9.15
timezone = "Europe/Berlin"
"""

_NO_INLINE_CHANNELS = """
type = "float"

[only_in_file]
name = "Only channel, only in system.d/channels.conf"
"""


def _write_project(tmp_path, system: str, system_channels: str, machines: bool = True) -> None:
    conf = tmp_path / "conf"
    (conf / "system.d").mkdir(parents=True)
    (tmp_path / "settings.conf").write_text(_SETTINGS)
    (conf / "system.conf").write_text(system)
    (conf / "system.d" / "channels.conf").write_text(system_channels)
    if machines:
        (conf / "machines.d").mkdir()
        (conf / "machines.conf").write_text(_MACHINES)
        (conf / "machines.d" / "channels.conf").write_text(_MACHINES_CHANNELS)


def _load_system(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["probe", "-c", str(tmp_path / "conf"), "-d", str(tmp_path), "run"])
    app = Application.load("probe", parser=ArgumentParser())
    return next(iter(app.components.values()))


def _rendered_modal_text(component) -> str:
    pytest.importorskip("dash")
    from lories.application.view.pages.widgets.configs_editor import _build_modal_body

    body = _build_modal_body(
        component.configs,
        type(component),
        component.id,
        list(component.components.values()),
        list(component.connectors.values()),
        None,
        None,
    )
    return json.dumps(body, default=lambda o: o.to_plotly_json() if hasattr(o, "to_plotly_json") else str(o))


@pytest.fixture
def system(tmp_path, monkeypatch):
    _write_project(tmp_path, _SYSTEM, _SYSTEM_CHANNELS)
    return _load_system(tmp_path, monkeypatch)


def test_runtime_registers_channels_from_both_sources(system):
    assert sorted(c.key for c in system.data.values()) == ["dfile_ch", "inline_ch"]
    machines = system.components.get_first()
    assert sorted(c.key for c in machines.data.values()) == ["both", "dfile_m", "inline_m"]


def test_file_channels_are_stored_in_configs(system):
    channels = system.configs["data"]["channels"]
    assert "dfile_ch" in channels.members
    assert channels["dfile_ch"]["name"] == "Channel from system.d/channels.conf"
    # file-level defaults are folded into the stored section, as the channel was created with them ...
    assert channels["dfile_ch"]["aggregate"] == "mean"
    assert channels["dfile_ch"]["logger"]["table"] == "probe"
    # ... but do not leak onto the channels of the main file
    assert "aggregate" not in channels
    assert "aggregate" not in channels["inline_ch"]
    assert "logger" not in channels["inline_ch"]

    machines = system.components.get_first()
    machine_channels = machines.configs["data"]["channels"]
    assert machine_channels["dfile_m"]["name"] == "Machine channel from machines.d/channels.conf"
    assert machine_channels["dfile_m"]["freq"] == "30s"
    assert "freq" not in machine_channels["inline_m"]


def test_file_defaults_sections_are_not_stored_as_channels(system):
    channels = system.configs["data"]["channels"]
    assert "logger" not in channels.members
    assert not any(c.key == "logger" for c in system.data.values())


def test_file_section_overrides_inline_section_like_the_runtime(system):
    machines = system.components.get_first()
    channel = machines.data.both
    stored = machines.configs["data"]["channels"]["both"]
    assert channel.name == "Overridden by machines.d/channels.conf"
    assert stored["name"] == "Overridden by machines.d/channels.conf"
    # keys only the inline section had survive, the file-level default is folded in
    assert stored["unit"] == "W"
    assert stored["freq"] == "30s"


def test_file_channels_without_inline_section_create_the_member(tmp_path, monkeypatch):
    _write_project(tmp_path, _NO_INLINE, _NO_INLINE_CHANNELS, machines=False)
    system = _load_system(tmp_path, monkeypatch)

    assert [c.key for c in system.data.values()] == ["only_in_file"]
    channels = system.configs["data"]["channels"]
    assert channels.members == ["only_in_file"]
    assert channels["only_in_file"]["type"] == "float"


def test_dash_config_modal_shows_file_channels(system):
    text = _rendered_modal_text(system)
    assert "Channel from system.d/channels.conf" in text
    assert "Inline channel (system.conf)" in text

    machines = system.components.get_first()
    text = _rendered_modal_text(machines)
    assert "Machine channel from machines.d/channels.conf" in text
