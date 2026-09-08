# -*- coding: utf-8 -*-
"""
tests.test_openems_binding
~~~~~~~~~~~~~~~~~~~~~~~~~~

Boots a real, headless ``lories`` application from a temporary project to drive
``OpenEMSBinding`` against canned per-connector discovery listings: a ``POINTS`` entry
becomes a ``component`` / ``channel`` / ``connector`` triple with a ``scale`` only where
the entry carries a factor, every bound address must exist on the connector that channel
actually resolved to, and the unit the device reports must match the native unit the
channel's own unit and scale imply - with the cumulated-energy marker normalized away.
"""

from __future__ import annotations

import os

import pytest

from lories.components import register_component_type
from lories.components.binding import BindableComponent
from lories.connectors.openems import ChannelInfo, OpenEMSBinding, OpenEMSConnector
from lories.core import Constant
from lories.core.configs import ConfigurationError
from lories.typing import Configurations

POWER = Constant(float, "power", "Power", "W", context="oemsbind")
CURRENT = Constant(float, "current", "Current", "A", context="oemsbind")
ENERGY = Constant(float, "energy", "Energy", "Wh", context="oemsbind")
STATE = Constant(int, "state", "State", "", context="oemsbind")
NOTE = Constant(float, "note", "Note", "W", context="oemsbind")

_SETTINGS_CONF = 'name = "oemsbind"\naction = "run"\n\n[interface]\nenabled = false\n'
_SYSTEM_CONF = (
    'key = "sys"\nname = "OpenEMS Binding Test System"\n'
    '\n[connectors.oe]\ntype = "openems_edge"\n'
    '\n[connectors.oe2]\ntype = "openems_edge"\n'
)
_DEVICE_CONF = 'device = "meter0"\nconnector = "oe"\n'

_DEFAULT_POINTS = {
    POWER: "ActivePower",
    CURRENT: ("Current", 0.001),  # the Edge publishes mA, the vocabulary is A
    ENERGY: "ActiveProductionEnergy",
    STATE: "State",
}


def _info(address: str, type: str, unit: str) -> ChannelInfo:  # noqa: A002
    component, channel = address.split("/", 1)
    return ChannelInfo(address=address, component=component, channel=channel, type=type, unit=unit, access_mode="RO")


def _listing(*infos: ChannelInfo):
    return {info.address: info for info in infos}


# The two connectors deliberately offer different channels: a validation that assumed one
# connector for the whole device would check somebody's channels against the wrong listing.
_EDGE_LISTING = _listing(
    _info("meter0/ActivePower", "INTEGER", "W"),
    _info("meter0/Current", "INTEGER", "mA"),
    _info("meter0/CurrentAmps", "INTEGER", "A"),
    _info("meter0/ActiveProductionEnergy", "LONG", "Wh_Σ"),
    _info("meter0/State", "INTEGER", None),
)
_ALT_LISTING = _listing(_info("meter0/ActivePower", "INTEGER", "W"))
_LISTINGS = {"oe": _EDGE_LISTING, "oe2": _ALT_LISTING}


@register_component_type("oemsbindtest")
class OpenEMSBindTestDevice(OpenEMSBinding, BindableComponent):
    POINTS = _DEFAULT_POINTS

    def _add_channels(self, configs: Configurations) -> None:
        for constant in (POWER, CURRENT, ENERGY, STATE, NOTE):
            self._add_channel(constant)


@pytest.fixture
def load_project(tmp_path, monkeypatch):
    """Write a headless project into ``tmp_path`` and load it; returns the bound device."""

    monkeypatch.setattr(OpenEMSConnector, "discover", lambda self, refresh=False: _LISTINGS[self.key])

    def _load(device_conf: str = _DEVICE_CONF):
        import lories

        conf_dir = tmp_path / "conf"
        conf_dir.mkdir(exist_ok=True)
        (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
        (conf_dir / "system.conf").write_text(_SYSTEM_CONF)
        (conf_dir / "oemsdev.conf").write_text('type = "oemsbindtest"\nname = "Bound Meter"\n' + device_conf)

        cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            app = lories.load("oemsbind")
        finally:
            os.chdir(cwd)

        devices = [c for c in app.components.values() if isinstance(c, OpenEMSBindTestDevice)]
        assert len(devices) == 1, [c.id for c in app.components.values()]
        return devices[0]

    return _load


def test_bind_emits_component_channel_connector_and_scale_only_for_tuples(load_project):
    device = load_project()

    power = device.data[POWER.key]
    assert power.get("component") == "meter0"
    assert power.get("channel") == "ActivePower"
    assert power.connector.id == "sys.oe"
    assert power.get("scale") is None

    current = device.data[CURRENT.key]
    assert current.get("channel") == "Current"
    assert current.get("scale") == 0.001

    # Constants outside POINTS stay unbound and are not validated
    assert not device.data[NOTE.key].has_connector()


def test_cumulated_energy_unit_is_accepted_for_a_wh_constant(load_project):
    device = load_project()

    assert device.data[ENERGY.key].get("channel") == "ActiveProductionEnergy"
    assert _EDGE_LISTING["meter0/ActiveProductionEnergy"].unit.endswith("Σ")


def test_missing_device_key_raises_naming_the_class(load_project):
    with pytest.raises(ConfigurationError, match="OpenEMSBindTestDevice"):
        load_project('connector = "oe"\n')


def test_missing_addresses_are_all_listed(load_project, monkeypatch):
    monkeypatch.setattr(
        OpenEMSBindTestDevice,
        "POINTS",
        {POWER: "ActivePower", CURRENT: "NoSuchChannel", ENERGY: "AlsoMissing"},
    )

    with pytest.raises(ConfigurationError) as excinfo:
        load_project()

    message = str(excinfo.value)
    assert "meter0/NoSuchChannel" in message
    assert "meter0/AlsoMissing" in message
    assert "meter0/ActivePower" not in message


def test_unit_mismatch_raises_naming_channel_discovered_and_expected(load_project, monkeypatch):
    monkeypatch.setattr(OpenEMSBindTestDevice, "POINTS", {POWER: "Current"})

    with pytest.raises(ConfigurationError) as excinfo:
        load_project()

    message = str(excinfo.value)
    assert "'power'" in message
    assert "'mA'" in message
    assert "'W'" in message


def test_milli_unit_is_rejected_without_the_matching_scale(load_project, monkeypatch):
    monkeypatch.setattr(OpenEMSBindTestDevice, "POINTS", {CURRENT: "Current"})

    with pytest.raises(ConfigurationError, match="'mA'"):
        load_project()


def test_unitless_discovered_channel_skips_the_unit_check(load_project, monkeypatch):
    monkeypatch.setattr(OpenEMSBindTestDevice, "POINTS", {POWER: "State"})

    device = load_project()
    assert device.data[POWER.key].get("channel") == "State"


def test_toml_overrides_are_validated_per_channel_and_per_connector(load_project):
    # 'power' moves to a second connector whose listing has nothing else; the remaining
    # channels must still be checked against their own connector's listing. 'current' is
    # repointed at the amp channel and its scale dropped, which only validates because the
    # check reads the channel's configured unit and scale, not the POINTS table.
    device = load_project(
        _DEVICE_CONF + '\n[data.channels.power]\nconnector = "oe2"\n'
        '\n[data.channels.current]\nchannel = "CurrentAmps"\nscale = 1.0\n'
    )

    assert device.data[POWER.key].connector.id == "sys.oe2"
    assert device.data[CURRENT.key].connector.id == "sys.oe"
    assert device.data[CURRENT.key].get("channel") == "CurrentAmps"
    assert device.data[CURRENT.key].get("scale") == 1.0


def test_channel_without_a_composable_address_raises_naming_the_key(load_project, monkeypatch):
    monkeypatch.setattr(
        OpenEMSBindTestDevice,
        "_bind",
        lambda self, constant: {"connector": self._connector_id} if constant is POWER else {},
    )

    with pytest.raises(ConfigurationError, match="'power' has no OpenEMS address"):
        load_project()


def test_validate_discovered_hook_is_called_with_the_listing(load_project, monkeypatch):
    seen = []

    monkeypatch.setattr(OpenEMSBindTestDevice, "_validate_discovered", lambda self, discovered: seen.append(discovered))

    load_project()

    assert seen == [_EDGE_LISTING]


def test_expected_unit_derives_the_native_unit_from_the_unit_and_scale():
    device = OpenEMSBindTestDevice.__new__(OpenEMSBindTestDevice)

    assert device._expected_unit("W", None) == "W"
    assert device._expected_unit("W", -1) == "W"  # a sign flip changes no unit
    assert device._expected_unit("A", 0.001) == "mA"
    assert device._expected_unit("W", 1000) == "kW"
    assert device._expected_unit("W", 3.6) is None  # no prefix derivable, skip the check
    assert device._expected_unit("", 0.001) is None  # nothing to compare
    assert device._expected_unit("W", "not-a-number") is None


def test_mixin_without_a_bindable_component_base_is_rejected():
    with pytest.raises(TypeError, match="OpenEMSBinding"):

        class Broken(OpenEMSBinding):
            pass


def test_mixin_must_precede_the_component_in_the_mro():
    with pytest.raises(TypeError, match="must list OpenEMSBinding before"):

        class Reversed(BindableComponent, OpenEMSBinding):
            pass
