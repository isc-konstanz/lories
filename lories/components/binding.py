# -*- coding: utf-8 -*-
"""
lories.components.binding
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from lories.components.component import Component
from lories.core import ConfigurationError, Constant
from lories.core.typing import Connector
from lories.typing import Configurations


class BindableComponent(Component):
    """
    Base for components that declare their channels from Constants and let a subclass wire them
    to a protocol. `_add_channels` declares the channels through `_add_channel`, which merges
    whatever `_bind` returns for a constant (point names, addresses, the connector id) into the
    channel config and leaves the channel unbound when `_bind` returns nothing.

    Connector wiring follows four rules. The connector id is the device's `connector` key; a
    binding family that requires a connector declares its registered connector type strings in
    `CONNECTOR_TYPES`, and configuring without a `connector` key then fails fast. If the
    device's own config declares `[connectors.<id>]`, the family defaults from
    `_connector_defaults` are merged into that block with the config's own keys winning. If it
    does not, the id resolves upward through the parents, so several devices can share one
    gateway; nothing is created. Once the connectors are loaded, every channel a binding claimed
    must resolve its connector, otherwise configuring fails and names the id; and if the family
    declares `CONNECTOR_TYPES`, the resolved connector must be an instance of one of them.
    """

    CONNECTOR_TYPES: tuple = ()

    _connector_id: Optional[str] = None
    _bound: List[str]

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._bound = []
        self._configure_bindings(configs)
        self._configure_connector(configs)
        self._add_channels(configs)

    def _configure_bindings(self, configs: Configurations) -> None:
        """Read the addressing a binding needs, before any channel is added; call `super()` first."""
        connector = configs.get("connector", default=None)
        if connector is not None and not isinstance(connector, str):
            raise ConfigurationError(
                f"{type(self).__name__} '{self.id}' expects 'connector' to be a connector id string, "
                f"not {type(connector).__name__}"
            )
        self._connector_id = connector if isinstance(connector, str) and len(connector) > 0 else None
        if type(self).CONNECTOR_TYPES and self._connector_id is None:
            raise ConfigurationError(
                f"{type(self).__name__} '{self.id}' requires a connector of type "
                f"{type(self).CONNECTOR_TYPES}: set 'connector' to an existing id"
            )

    def _configure_connector(self, configs: Configurations) -> None:
        if self._connector_id is None or not configs.has_member("connectors"):
            return
        if configs.get_member("connectors").has_member(self._connector_id):
            self.connectors.add(self._connector_id, **self._connector_defaults())

    # noinspection PyMethodMayBeStatic
    def _connector_defaults(self) -> Dict[str, Any]:
        """Family defaults for a locally declared connector; the config's own keys win."""
        return {}

    # noinspection PyMethodMayBeStatic, PyUnusedLocal
    def _bind(self, constant: Constant) -> Dict[str, Any]:
        """Return the binding for a constant, or an empty dict to leave the channel unbound."""
        return {}

    def _add_channel(self, constant: Constant, **custom: Any) -> None:
        channel = constant.to_dict()
        channel["name"] = f"{self.name} {constant.name}"
        channel.update(self._bind(constant))
        channel.update(custom)
        if channel.get("connector") is not None:
            self._bound.append(constant.key)
        self.data.add(**channel)

    def _add_data(self, key: str, **configs: Any) -> None:
        if configs.get("connector") is not None:
            self._bound.append(key)
        self.data.add(key, **configs)

    def _add_channels(self, configs: Configurations) -> None:
        """Declare the component's channels through `_add_channel`."""

    def _on_configure(self, configs: Configurations) -> None:
        super()._on_configure(configs)
        self._check_bound(getattr(self, "_bound", []))

    def _check_bound(self, keys: Iterable[str]) -> None:
        """Assert the named bound channels resolved a connector, of this family if one is declared.

        `_on_configure` runs this over everything a binding claimed. A component that adds
        channels later - once the connectors exist, e.g. from a device listing - runs it again
        over those keys, because they were not declared yet when the seam checked.
        """
        keys = list(keys)
        unresolved = [key for key in keys if not self.data[key].has_connector()]
        if unresolved:
            raise ConfigurationError(
                f"{type(self).__name__} '{self.id}' cannot resolve connector '{self._connector_id}' for channels "
                f"{unresolved}: declare [connectors.{self._connector_id}] in this component's configuration or in "
                f"a parent's, or set 'connector' to an existing id"
            )
        if type(self).CONNECTOR_TYPES:
            from lories.connectors import registry as connector_registry

            families = tuple(connector_registry.from_type(name).type for name in type(self).CONNECTOR_TYPES)
            for key in keys:
                channel = self.data[key]
                connector = channel.connector._connector
                if not isinstance(connector, families):
                    raise ConfigurationError(
                        f"{type(self).__name__} '{self.id}' channel '{key}' connector '{self._connector_id}' "
                        f"resolved to {type(connector).__name__}, expected one of {type(self).CONNECTOR_TYPES}"
                    )

    def _resolve_connector(self) -> Optional[Connector]:
        """Resolve the device's `connector` id to the live connector, or None when nothing matches.

        Mirrors how a channel resolves its own connector (`_ChannelWrapper._build_registrator`):
        a bare id is tried against every prefix of this component's id path, innermost first,
        and then as given, so an application-level `[connectors.<id>]` resolves as well as one
        declared here or on a parent. A dotted id is only looked up as written.
        """
        if self._connector_id is None:
            return None
        context = self.connectors.context
        connector_id = self._connector_id
        if "." not in connector_id:
            for i in reversed(range(1, len(self.path) + 1)):
                _id = ".".join([*self.path[:i], connector_id])
                if _id in context.keys():
                    connector_id = _id
                    break
        return context.get(connector_id, None)
