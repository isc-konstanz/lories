# -*- coding: utf-8 -*-
"""
lories.connectors.openems.binding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

from lories.connectors.openems.client import ChannelInfo, OpenEMSConnector
from lories.core import ConfigurationError, Constant
from lories.typing import Configurations

#: OpenEMS marks cumulated (energy) channels with a Greek capital sigma appended to their unit.
CUMULATED_SUFFIX = "_\u03a3"


class OpenEMSBinding:
    """
    Mixin that binds a `BindableComponent` vocabulary to one OpenEMS component. Subclasses
    declare the OpenEMS channel name for each constant they can read; every constant left out
    of `POINTS` stays unbound, to be filled by TOML or by another component. A `POINTS` entry
    may be a plain channel name, or a `(channel, scale)` tuple when the value the Edge publishes
    needs a linear factor to reach the vocabulary's unit and sign convention (mA to A is 0.001,
    a reference flip is -1).

    Wiring a device needs the OpenEMS component id its channels live under and the connector
    id (`connector`); the address a channel subscribes to is composed as `<component>/<channel>`.
    That component id is spelled `device`, like the SunSpec unit id the sibling mixin addresses
    a device by: `component` is a reserved lories config key (`Component.TYPE`), and a flat
    `component = "meter0"` is replaced by the registrator's own `[component]` member while the
    component is being built, long before `configure()` sees it. Because a binding is written as
    ordinary channel config, TOML can still override `component`, `channel`, `connector` or the
    `converter` table (`converter = { type = "linear", scale = ... }`) per channel.

    At configure time, once the connectors exist, every bound address is checked against the
    connector's REST discovery listing, and the unit the device reports is checked against the
    native unit the channel's own unit and its linear converter's `scale` imply - the channel's,
    not the table's, so a TOML override of `converter`, `unit`, `channel` or `connector` is
    validated as configured.
    Both mismatches fail the start. Discovery is a REST call on the Edge, so bound devices are
    `openems_edge` only; a Backend connector has no listing to check against.

    The mixin lives next to its connector, so with `websocket-client` missing it is marked
    unavailable together with the `openems_edge` connector, and every device class that mixes
    it in becomes unavailable too. That is intended: without the connector there is nothing for
    such a device to bind to.
    """

    CONNECTOR_TYPES = ("openems_edge",)

    POINTS: Dict[Constant, Union[str, Tuple[str, float]]] = {}

    component: str

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        from lories.components.binding import BindableComponent

        if not issubclass(cls, BindableComponent):
            raise TypeError(f"{cls.__name__} mixes in OpenEMSBinding without a BindableComponent base")
        mro = cls.__mro__
        if mro.index(OpenEMSBinding) > mro.index(BindableComponent):
            raise TypeError(f"{cls.__name__} must list OpenEMSBinding before BindableComponent")

    def _configure_bindings(self, configs: Configurations) -> None:
        super()._configure_bindings(configs)
        component = configs.get("device", default=None)
        if not isinstance(component, str) or len(component) == 0:
            raise ConfigurationError(
                f"{type(self).__name__} '{self.id}' requires the OpenEMS component id its channels live under: "
                "set 'device', e.g. device = 'meter0'"
            )
        self.component = component

    def _bind(self, constant: Constant) -> Dict[str, Any]:
        point = self._point(constant)
        if point is None:
            return {}
        binding = {
            "component": self.component,
            "channel": point,
            "connector": self._connector_id,
        }
        scale = self._scale(constant)
        if scale is not None:
            binding["converter"] = {"type": "linear", "scale": scale}
        return binding

    def _point(self, constant: Constant) -> Optional[str]:
        entry = type(self).POINTS.get(constant)
        if entry is None:
            return None
        return entry[0] if isinstance(entry, tuple) else entry

    def _scale(self, constant: Constant) -> Optional[float]:
        entry = type(self).POINTS.get(constant)
        if isinstance(entry, tuple):
            return entry[1]
        return None

    def _expected_unit(self, unit: Optional[str], scale: Optional[float]) -> Optional[str]:
        """The unit the Edge must report for a channel, or None to skip the check.

        The channel's unit is what it carries *after* the scale was applied, so the native unit
        follows from the factor: 0.001 means the device publishes milli-units, 1000 that it
        publishes kilo-units. A factor that is neither of those, nor a plain sign flip, has no
        derivable unit and skips the check, as does a channel without a unit.
        """
        if not unit:
            return None
        if scale is None:
            return unit
        try:
            factor = abs(float(scale))
        except (TypeError, ValueError):
            return None
        if factor == 1:
            return unit
        if factor == 0.001:
            return f"m{unit}"
        if factor == 1000:
            return f"k{unit}"
        return None

    def _on_configure(self, configs: Configurations) -> None:
        super()._on_configure(configs)
        self._validate_bindings()

    def _validate_bindings(self) -> None:
        for connector, keys in self._binding_groups():
            discovered = connector.discover()

            addresses = {key: self._binding_address(key) for key in keys}
            missing = sorted({a for a in addresses.values() if a not in discovered})
            if missing:
                raise ConfigurationError(
                    f"{type(self).__name__} '{self.id}' binds OpenEMS channels connector '{connector.id}' does not "
                    f"offer: {missing}; check the 'device' id and the bound channel names"
                )

            for key, address in addresses.items():
                self._validate_unit(key, address, discovered)

            self._validate_discovered(discovered)

    def _validate_unit(self, key: str, address: str, discovered: Dict[str, ChannelInfo]) -> None:
        channel = self.data[key]
        expected = self._expected_unit(channel.unit, channel.converter.get("scale"))
        if expected is None:
            return
        unit = discovered[address].unit
        if not unit:
            return
        if unit.endswith(CUMULATED_SUFFIX):
            unit = unit[: -len(CUMULATED_SUFFIX)]
        if unit != expected:
            raise ConfigurationError(
                f"{type(self).__name__} '{self.id}' channel '{key}' is bound to '{address}', which the device "
                f"reports in '{unit}' while the binding expects '{expected}'"
            )

    # noinspection PyUnusedLocal
    def _validate_discovered(self, discovered: Dict[str, ChannelInfo]) -> None:
        """Hook for subclasses to check the discovery listing beyond addresses and units."""

    def _binding_groups(self) -> List[Tuple[OpenEMSConnector, List[str]]]:
        """The bound channels grouped by the connector each of them actually resolved to.

        TOML may point single channels at another connector, so the listing a channel is
        checked against is the one its own connector offers, not the first connector found.
        """
        groups: Dict[str, Tuple[OpenEMSConnector, List[str]]] = {}
        for key in self._bound:
            channel = self.data[key]
            if not channel.has_connector():
                continue
            connector = channel.connector._connector
            groups.setdefault(connector.id, (connector, []))[1].append(key)
        return list(groups.values())

    def _binding_address(self, key: str) -> str:
        channel = self.data[key]
        component = channel.get("component")
        name = channel.get("channel")
        if not component or not name:
            raise ConfigurationError(
                f"{type(self).__name__} '{self.id}' channel '{key}' has no OpenEMS address: both 'component' "
                f"and 'channel' must be set, got '{component}' / '{name}'"
            )
        return f"{component}/{name}"
