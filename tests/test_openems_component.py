# -*- coding: utf-8 -*-
"""
tests.test_openems_component
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Boots a real, headless ``lories`` application from a temporary project to drive the
generic ``openems`` mirror component: it names its connector by id (locally declared or
resolved upward), expands its allowlist against the connector's REST discovery listing
once the connectors exist, and the channels it adds that late are real, connector-bound
channels of the application's data context.
"""

from __future__ import annotations

import json
import os

import pytest

_SETTINGS_CONF = 'name = "openems_test"\naction = "run"\n\n[interface]\nenabled = false\n'
_SYSTEM_CONF = 'key = "sys"\nname = "OpenEMS Test System"\n'
_EDGE = '\n[connectors.oe]\ntype = "openems_edge"\nhost = "openems.example"\n'

_DISCOVERED_CHANNELS = [
    {"address": "_sum/GridActivePower", "type": "INTEGER", "unit": "W", "accessMode": "RO"},
    {"address": "_sum/EssSoc", "type": "INTEGER", "unit": "%", "accessMode": "RO"},
    {"address": "_sum/ProductionActivePower", "type": "INTEGER", "unit": "W", "accessMode": "RO"},
    {"address": "ess0/Soc", "type": "INTEGER", "unit": "%", "accessMode": "RO"},
    {"address": "meter0/ActivePower", "type": "FLOAT", "unit": "W", "accessMode": "RO"},
]
_DISCOVERED_CHANNELS_JSON = json.dumps(_DISCOVERED_CHANNELS)


def _rest_ok(self, path, params=None):
    return _DISCOVERED_CHANNELS_JSON


@pytest.fixture
def load_project(tmp_path):
    """Write a headless project into ``tmp_path`` and load it; returns ``(app, component)``."""

    def _load(openems_conf: str, system_extra: str = "", settings_extra: str = ""):
        import lories
        from lories.components.openems import OpenEMSComponent

        conf_dir = tmp_path / "conf"
        conf_dir.mkdir(exist_ok=True)
        (conf_dir / "settings.conf").write_text(_SETTINGS_CONF + settings_extra)
        (conf_dir / "system.conf").write_text(_SYSTEM_CONF + system_extra)
        (conf_dir / "openems.conf").write_text('type = "openems"\n' + openems_conf)

        cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            app = lories.load("openems_test")
        finally:
            os.chdir(cwd)

        components = [c for c in app.components.values() if isinstance(c, OpenEMSComponent)]
        assert len(components) == 1, [c.id for c in app.components.values()]
        return app, components[0]

    return _load


def test_missing_connector_key_raises_naming_the_component(load_project):
    from lories.core.configs.errors import ConfigurationError

    with pytest.raises(ConfigurationError, match="OpenEMSComponent"):
        load_project('channels = ["ess0/Soc"]\n')


def test_upstream_connector_resolves_and_late_added_channels_are_bound(load_project, monkeypatch):
    from lories.io.rest import Rest

    monkeypatch.setattr(Rest, "get_request", _rest_ok)

    app, component = load_project(
        'connector = "oe"\nchannels = ["_sum/GridActivePower", "ess0/Soc"]\n', system_extra=_EDGE
    )

    channels = list(component.data.values())
    assert len(channels) == 2

    grid = component.data.get("sum_gridactivepower")
    assert grid.get("component") == "_sum"
    assert grid.get("channel") == "GridActivePower"
    assert grid.type is int
    assert grid.unit == "W"

    soc = component.data.get("ess0_soc")
    assert soc.get("component") == "ess0"
    assert soc.get("channel") == "Soc"
    assert soc.type is int
    assert soc.unit == "%"

    # The channels were added after Component._on_configure: they must still be real,
    # connector-bound channels of the application's data context.
    for channel in channels:
        assert channel.has_connector()
        assert channel.connector.id == "sys.oe"
    assert {c.id for c in channels} <= {c.id for c in app.channels}
    assert app.channels["sys.openems.ess0_soc"].connector._connector is app.connectors["sys.oe"]


def test_application_level_connector_resolves_by_bare_id(load_project, monkeypatch):
    # Declared in settings.conf, so its id is the bare "oe" - no parent prefix of the
    # component's path matches it, and only the fallback to the id as written resolves it.
    from lories.io.rest import Rest

    monkeypatch.setattr(Rest, "get_request", _rest_ok)

    _, component = load_project('connector = "oe"\nchannels = ["ess0/Soc"]\n', settings_extra=_EDGE)

    assert component.data.get("ess0_soc").connector.id == "oe"


def test_local_connector_declaration_resolves(load_project, monkeypatch):
    from lories.io.rest import Rest

    monkeypatch.setattr(Rest, "get_request", _rest_ok)

    _, component = load_project('connector = "oe"\nchannels = ["ess0/Soc"]\n\n[connectors.oe]\ntype = "openems_edge"\n')

    assert component.data.get("ess0_soc").connector.id == "sys.openems.oe"


def test_connector_of_another_family_raises_naming_the_class(load_project):
    from lories.core.configs.errors import ConfigurationError

    with pytest.raises(ConfigurationError, match="VirtualConnector"):
        load_project(
            'connector = "oe"\nchannels = ["ess0/Soc"]\n',
            system_extra='\n[connectors.oe]\ntype = "virtual"\n',
        )


def test_underscore_prefixed_pattern_is_not_hidden(load_project, monkeypatch):
    from lories.io.rest import Rest

    monkeypatch.setattr(Rest, "get_request", _rest_ok)

    _, component = load_project('connector = "oe"\nchannels = ["_sum/*"]\n', system_extra=_EDGE)

    keys = {channel.key for channel in component.data.values()}
    assert keys == {"sum_gridactivepower", "sum_esssoc", "sum_productionactivepower"}


def test_dead_pattern_raises_configuration_error(load_project, monkeypatch):
    from lories.core.configs.errors import ConfigurationError
    from lories.io.rest import Rest

    monkeypatch.setattr(Rest, "get_request", _rest_ok)

    with pytest.raises(ConfigurationError, match="doesnotexist/\\*"):
        load_project('connector = "oe"\nchannels = ["doesnotexist/*"]\n', system_extra=_EDGE)


def test_two_addresses_normalizing_to_one_key_raise_naming_both(load_project, monkeypatch):
    from lories.core.configs.errors import ConfigurationError
    from lories.io.rest import Rest

    colliding = json.dumps(
        [
            {"address": "meter0/Active.Power", "type": "FLOAT", "unit": "W"},
            {"address": "meter0/Active_Power", "type": "FLOAT", "unit": "W"},
        ]
    )
    monkeypatch.setattr(Rest, "get_request", lambda self, path, params=None: colliding)

    with pytest.raises(ConfigurationError) as excinfo:
        load_project('connector = "oe"\nchannels = ["meter0/*"]\n', system_extra=_EDGE)

    message = str(excinfo.value)
    assert "meter0/Active.Power" in message
    assert "meter0/Active_Power" in message
    assert "meter0_active_power" in message


def test_rest_failure_after_retries_raises_connector_error(load_project, monkeypatch):
    import lories.connectors.openems.client as openems_client
    from lories.connectors.errors import ConnectorError
    from lories.io.rest import Rest

    attempts = []

    def _rest_fail(self, path, params=None):
        attempts.append(path)
        raise ConnectionError("REST unreachable")

    monkeypatch.setattr(Rest, "get_request", _rest_fail)
    monkeypatch.setattr(openems_client.time, "sleep", lambda seconds: None)

    with pytest.raises(ConnectorError, match="discovery failed"):
        load_project('connector = "oe"\nchannels = ["_sum/*"]\n', system_extra=_EDGE)

    assert len(attempts) == 3


def test_log_available_channels_lists_and_aborts(load_project, monkeypatch):
    # The lories Settings boot reconfigures logging, so caplog cannot capture the
    # component logger here; the listing format itself is covered by the unit
    # test below and the abort message carries the discovered count.
    from lories.core.configs.errors import ConfigurationError
    from lories.io.rest import Rest

    monkeypatch.setattr(Rest, "get_request", _rest_ok)

    with pytest.raises(ConfigurationError, match="Listed 5 available OpenEMS channels"):
        load_project('connector = "oe"\nlog_available_channels = true\n', system_extra=_EDGE)


def test_log_available_channels_wins_over_configured_channels(load_project, monkeypatch):
    from lories.core.configs.errors import ConfigurationError
    from lories.io.rest import Rest

    monkeypatch.setattr(Rest, "get_request", _rest_ok)

    with pytest.raises(ConfigurationError, match="remove log_available_channels"):
        load_project('connector = "oe"\nchannels = ["_sum/*"]\nlog_available_channels = true\n', system_extra=_EDGE)


def test_empty_channels_without_flag_raises_with_hint(load_project):
    from lories.core.configs.errors import ConfigurationError

    with pytest.raises(ConfigurationError, match="No OpenEMS channels configured"):
        load_project('connector = "oe"\n', system_extra=_EDGE)


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


def test_log_available_channels_listing_format():
    from lories.components.openems import OpenEMSComponent
    from lories.connectors.openems import OpenEMSConnector

    records = []

    class _Recorder:
        def info(self, message):
            records.append(message)

    discovered = {}
    for entry in _DISCOVERED_CHANNELS:
        info = OpenEMSConnector._build_info(entry)
        discovered[info.address] = info

    component = OpenEMSComponent.__new__(OpenEMSComponent)
    component._logger = _Recorder()
    component._log_available_channels(discovered)

    listing = "\n".join(records)
    assert "5 OpenEMS channels available in 3 components" in listing
    assert "  _sum/: EssSoc [INTEGER, %], GridActivePower [INTEGER, W], ProductionActivePower [INTEGER, W]" in listing
    assert "  ess0/: Soc [INTEGER, %]" in listing
    assert "  meter0/: ActivePower [FLOAT, W]" in listing
