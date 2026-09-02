# -*- coding: utf-8 -*-
"""
tests.test_openems_component
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Boots a real ``lories`` application with a loose ``openems.conf`` allowlist
component and a mocked REST discovery response, driving the allowlist
expansion, fail-fast discovery, and connector-type resolution through the
real configuration and component wiring.
"""

from __future__ import annotations

import json
import os

import pytest

_SETTINGS_CONF = 'name = "openems_component_test"\naction = "run"\n\n[interface]\nenabled = false\n'

_DISCOVERED_CHANNELS = [
    {"address": "_sum/GridActivePower", "type": "INTEGER", "unit": "W"},
    {"address": "_sum/EssSoc", "type": "INTEGER", "unit": "%"},
    {"address": "_sum/ProductionActivePower", "type": "INTEGER", "unit": "W"},
    {"address": "ess0/Soc", "type": "INTEGER", "unit": "%"},
    {"address": "meter0/ActivePower", "type": "FLOAT", "unit": "W"},
]
_DISCOVERED_CHANNELS_JSON = json.dumps(_DISCOVERED_CHANNELS)


def _rest_ok(self, path, params=None):
    return _DISCOVERED_CHANNELS_JSON


def _boot_openems(tmp_path, openems_conf: str):
    """Boot a real lories ``Application`` with one loose openems component and return it."""
    from lories.application import Settings
    from lories.application.main import Application
    from lories.components.openems import OpenEMSComponent

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir(exist_ok=True)
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "openems.conf").write_text(openems_conf)

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        settings = Settings("openems_component_test")
        app = Application(settings)
        app.configure(settings)
    finally:
        os.chdir(cwd)

    components = app._components.get_all(OpenEMSComponent)
    assert len(components) == 1
    return components[0]


def _openems_conf(channels: str, connector: str = "") -> str:
    return f'type = "openems"\nchannels = {channels}\n\n[connector]\n{connector}\n'


def test_allowlist_expansion_creates_only_matched_channels(tmp_path, monkeypatch):
    from lories.io.rest import Rest

    monkeypatch.setattr(Rest, "get_request", _rest_ok)

    component = _boot_openems(tmp_path, _openems_conf('["_sum/GridActivePower", "ess0/Soc"]'))

    channels = list(component.data.values())
    assert len(channels) == 2

    grid = component.data.get("sum_gridactivepower")
    assert grid.address == "_sum/GridActivePower"
    assert grid.type is int
    assert grid.unit == "W"

    soc = component.data.get("ess0_soc")
    assert soc.address == "ess0/Soc"
    assert soc.type is int
    assert soc.unit == "%"


def test_underscore_prefixed_pattern_is_not_hidden(tmp_path, monkeypatch):
    from lories.io.rest import Rest

    monkeypatch.setattr(Rest, "get_request", _rest_ok)

    component = _boot_openems(tmp_path, _openems_conf('["_sum/*"]'))

    keys = {channel.key for channel in component.data.values()}
    assert keys == {"sum_gridactivepower", "sum_esssoc", "sum_productionactivepower"}


def test_dead_pattern_raises_configuration_error(tmp_path, monkeypatch):
    from lories.core.configs.errors import ConfigurationError
    from lories.io.rest import Rest

    monkeypatch.setattr(Rest, "get_request", _rest_ok)

    with pytest.raises(ConfigurationError, match="doesnotexist/\\*"):
        _boot_openems(tmp_path, _openems_conf('["doesnotexist/*"]'))


def test_rest_failure_after_retries_raises_configuration_error(tmp_path, monkeypatch):
    from lories.core.configs.errors import ConfigurationError
    from lories.io.rest import Rest

    attempts = []

    def _rest_fail(self, path, params=None):
        attempts.append(path)
        raise ConnectionError("REST unreachable")

    monkeypatch.setattr(Rest, "get_request", _rest_fail)
    monkeypatch.setattr("lories.components.openems.time.sleep", lambda seconds: None)

    with pytest.raises(ConfigurationError, match="discovery failed"):
        _boot_openems(tmp_path, _openems_conf('["_sum/*"]'))

    assert len(attempts) == 3


def test_unknown_connector_type_raises_configuration_error(tmp_path, monkeypatch):
    from lories.core.configs.errors import ConfigurationError
    from lories.io.rest import Rest

    monkeypatch.setattr(Rest, "get_request", _rest_ok)

    conf = _openems_conf('["_sum/*"]', connector='type = "not_a_real_connector"\n')
    with pytest.raises(ConfigurationError, match="not_a_real_connector"):
        _boot_openems(tmp_path, conf)


def test_map_type_maps_openems_type_strings():
    from lories.components.openems import OpenEMSComponent

    assert OpenEMSComponent._map_type("INTEGER") is int
    assert OpenEMSComponent._map_type("LONG") is int
    assert OpenEMSComponent._map_type("SHORT") is int
    assert OpenEMSComponent._map_type("FLOAT") is float
    assert OpenEMSComponent._map_type("DOUBLE") is float
    assert OpenEMSComponent._map_type("BOOLEAN") is bool
    assert OpenEMSComponent._map_type("STRING") is str
    assert OpenEMSComponent._map_type(None) is str


def test_make_key_normalizes_addresses():
    from lories.components.openems import OpenEMSComponent

    assert OpenEMSComponent._make_key("_sum", "GridActivePower") == "sum_gridactivepower"
    assert OpenEMSComponent._make_key("ess0", "Soc") == "ess0_soc"
    assert OpenEMSComponent._make_key("meter0", "Active.Power") == "meter0_active_power"
