# -*- coding: utf-8 -*-
"""
tests.test_components_binding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Boots a real, headless ``lories`` application from a temporary project to drive
``BindableComponent``'s connector wiring: a family that declares ``CONNECTOR_TYPES``
requires a ``connector`` key up front, a bare id resolves upward to a shared gateway of
the right family, a connector of the wrong family is rejected by class name, an id that
resolves nowhere fails naming it, a locally declared ``[connectors.<id>]`` receives the
family defaults with the config's own keys winning, and ``_add_data`` channels are
covered by the same unresolved check as ``_add_channel`` channels.
"""

from __future__ import annotations

import os
from typing import Any, Dict

import pytest

from lories.components import register_component_type
from lories.components.binding import BindableComponent
from lories.core import Constant
from lories.core.configs import ConfigurationError
from lories.typing import Configurations

READING = Constant(float, "reading", "Reading", "W", context="bindtest")
NOTE = Constant(float, "note", "Note", "", context="bindtest")

_SETTINGS_CONF = 'name = "bindtest"\naction = "run"\n\n[interface]\nenabled = false\n'
_SYSTEM_CONF = 'key = "sys"\nname = "Binding Test System"\n'
_VIRTUAL = 'type = "virtual"'
_CSV = 'type = "csv"'


@register_component_type("bindtest")
class BindTestDevice(BindableComponent):
    CONNECTOR_TYPES = ("virtual",)

    def _connector_defaults(self) -> Dict[str, Any]:
        return {"family": "bindtest-default", "shared": "from-class"}

    def _bind(self, constant: Constant) -> Dict[str, Any]:
        if constant is READING:
            return {"connector": self._connector_id, "address": "reg1"}
        return {}

    def _add_channels(self, configs: Configurations) -> None:
        self._add_channel(READING)
        self._add_channel(NOTE)
        self._add_data("mirror", type=float, connector=self._connector_id)


@pytest.fixture
def load_project(tmp_path):
    """Write a headless project into ``tmp_path`` and load it; returns the ``bindtest`` component."""

    def _load(device_conf: str, system_extra: str = ""):
        import lories

        conf_dir = tmp_path / "conf"
        conf_dir.mkdir(exist_ok=True)
        (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
        (conf_dir / "system.conf").write_text(_SYSTEM_CONF + system_extra)
        (conf_dir / "bindtest.conf").write_text('type = "bindtest"\nname = "Bind Test"\n' + device_conf)

        cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            app = lories.load("bindtest")
        finally:
            os.chdir(cwd)

        devices = [c for c in app.components.values() if isinstance(c, BindTestDevice)]
        assert len(devices) == 1, [c.id for c in app.components.values()]
        return devices[0]

    return _load


def test_connector_types_without_connector_key_raises(load_project):
    with pytest.raises(ConfigurationError, match="BindTestDevice"):
        load_project("")


def test_connector_table_instead_of_id_raises_naming_the_type(load_project):
    with pytest.raises(ConfigurationError, match="connector id string, not"):
        load_project('[connector]\ntype = "virtual"\n')


def test_bare_id_resolves_to_the_shared_upstream_connector(load_project):
    device = load_project('connector = "virt"\n', system_extra=f"\n[connectors.virt]\n{_VIRTUAL}\n")

    reading = device.data[READING.key]
    assert reading.has_connector()
    assert reading.connector.id == "sys.virt"
    # Nothing is created on the device when it declares no connector of its own
    assert len(device.connectors) == 0
    # Channels the binding did not claim stay unbound without complaint
    assert not device.data[NOTE.key].has_connector()


def test_wrong_family_connector_raises_naming_the_class(load_project):
    with pytest.raises(ConfigurationError, match="CsvDatabase"):
        load_project('connector = "virt"\n', system_extra=f"\n[connectors.virt]\n{_CSV}\n")


def test_unresolved_connector_fails_at_configure_and_names_add_data_channel(load_project):
    with pytest.raises(ConfigurationError, match="virt") as excinfo:
        load_project('connector = "virt"\n')
    assert "mirror" in str(excinfo.value)
    assert "reading" in str(excinfo.value)


def test_local_connector_receives_family_defaults_and_toml_wins(load_project):
    device = load_project(f'connector = "virt"\n\n[connectors.virt]\n{_VIRTUAL}\nshared = "from-toml"\n')

    assert device.data[READING.key].connector.id == "sys.bindtest.virt"
    (connector,) = device.connectors.values()
    assert connector.configs.get("family") == "bindtest-default"
    assert connector.configs.get("shared") == "from-toml"
