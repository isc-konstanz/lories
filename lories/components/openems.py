# -*- coding: utf-8 -*-
"""
lories.components.openems
~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

import fnmatch
import re
from typing import Dict, List, Optional

from lories.components import register_component_type
from lories.components.binding import BindableComponent
from lories.connectors.openems import ChannelInfo, OpenEMSEdgeConnector
from lories.core.configs.errors import ConfigurationError
from lories.core.configs.parameters import BoolParameter, ListParameter
from lories.typing import Configurations


@register_component_type("openems")
class OpenEMSComponent(BindableComponent):
    """A component that mirrors an explicit allowlist of OpenEMS channel addresses,
    expanded against the connector's REST discovery listing at configure time.

    Only addresses matching a configured pattern are created - there is no
    mirror-everything behaviour and no ``_``-prefix hiding; addresses such as
    ``_sum/GridActivePower`` must be named explicitly to be exposed.

    To explore a device before writing the allowlist, set
    ``log_available_channels = true``: every available channel is listed at
    INFO and the start is aborted; ``channels`` may be omitted in this mode.
    An empty or missing ``channels`` list without the flag is a configuration
    error.

    Like every bound component, this one names its connector by id and creates
    nothing: ``connector`` must resolve to an ``openems_edge`` connector
    declared here, in a parent, or at application level. Discovery is a REST
    call the Edge answers, so an ``openems_backend`` connector cannot back this
    component.

    Example ``system.conf``::

        [connectors.openems_edge]
        type          = "openems_edge"
        host          = "localhost"
        username      = "admin"
        password      = "admin"
        ws_port       = 8085
        rest_port     = 8084
        rest_endpoint = "rest"

        [components.openems]
        type      = "openems"
        connector = "openems_edge"
        channels  = ["_sum/Grid*", "ess0/Soc"]
    """

    CONNECTOR_TYPES = ("openems_edge",)

    _channels = ListParameter(
        key="channels",
        item_type=str,
        default=[],
        desc="OpenEMS channel address patterns (fnmatch glob, e.g. '_sum/Grid*') to create channels for",
    )
    _log_available = BoolParameter(
        key="log_available_channels",
        default=False,
        desc="Browse mode: list every channel the OpenEMS device offers at INFO, then abort the start",
    )

    def _add_channels(self, configs: Configurations) -> None:
        # The allowlist is expanded in _on_configure, once the connector that discovers it
        # exists; fail here already, so a component without any channel request never boots.
        if not self._channels and not self._log_available:
            raise ConfigurationError(
                "No OpenEMS channels configured; set log_available_channels = true to list what is available"
            )

    def _on_configure(self, configs: Configurations) -> None:
        super()._on_configure(configs)

        discovered = self._openems_connector().discover()
        self._logger.debug(f"OpenEMS channels available: {sorted(discovered)}")

        if self._log_available:
            self._log_available_channels(discovered)
            raise ConfigurationError(
                f"Listed {len(discovered)} available OpenEMS channels; remove log_available_channels to start normally"
            )

        matched = {}
        for pattern in self._channels:
            addresses = sorted(address for address in discovered if fnmatch.fnmatchcase(address, pattern))
            if not addresses:
                raise ConfigurationError(f"OpenEMS channel pattern '{pattern}' matched no discovered channels")
            for address in addresses:
                matched[address] = discovered[address]

        added: Dict[str, str] = {}
        for address in sorted(matched):
            channel = matched[address]
            key = self._make_key(channel.component, channel.channel)
            if key in added:
                raise ConfigurationError(
                    f"OpenEMS channels '{added[key]}' and '{address}' both normalize to the channel key "
                    f"'{key}'; narrow the patterns so only one of them is created"
                )
            added[key] = address
            self._add_data(
                key=key,
                name=f"{channel.component} / {channel.channel}",
                type=self._map_type(channel.type),
                unit=channel.unit,
                aggregate="last",
                component=channel.component,
                channel=channel.channel,
                connector=self._connector_id,
            )
        # Channels added after Component._on_configure ran are config entries only; loading
        # again instantiates them and resolves their connector, like any declared channel.
        # Re-loading is idempotent for the channels that already exist: DataContext
        # ._load_from_configs updates a known id instead of creating a second channel.
        self.data.load()
        self._check_bound(added)

        self._logger.info(f"Created {len(matched)} OpenEMS channels")
        self._logger.debug(f"OpenEMS channels: {sorted(matched)}")

    def _openems_connector(self) -> OpenEMSEdgeConnector:
        """The Edge connector this component mirrors, resolved before any channel exists."""
        connector = self._resolve_connector()
        if connector is None:
            raise ConfigurationError(
                f"{type(self).__name__} '{self.id}' cannot resolve connector '{self._connector_id}': declare "
                f"[connectors.{self._connector_id}] in this component's configuration, in a parent's or in the "
                f"settings, or set 'connector' to an existing id"
            )
        if not isinstance(connector, OpenEMSEdgeConnector):
            raise ConfigurationError(
                f"{type(self).__name__} '{self.id}' connector '{self._connector_id}' resolved to "
                f"{type(connector).__name__}, expected one of {type(self).CONNECTOR_TYPES}"
            )
        return connector

    def _log_available_channels(self, discovered: Dict[str, ChannelInfo]) -> None:
        """Log every discovered channel at INFO, one line per OpenEMS component."""
        by_comp: Dict[str, List[str]] = {}
        for channel in discovered.values():
            detail = ""
            if channel.type:
                detail = f" [{channel.type}{', ' + channel.unit if channel.unit else ''}]"
            by_comp.setdefault(channel.component, []).append(f"{channel.channel}{detail}")
        self._logger.info(
            f"{len(discovered)} OpenEMS channels available in {len(by_comp)} components "
            f"(pattern them into the 'channels' list, e.g. '_sum/Grid*'):"
        )
        for comp_id in sorted(by_comp):
            self._logger.info(f"  {comp_id}/: {', '.join(sorted(by_comp[comp_id]))}")

    @staticmethod
    def _map_type(openems_type: Optional[str]) -> type:
        """Map an OpenEMS channel type string to a Python type."""
        if openems_type is None:
            return str
        t = openems_type.upper()
        if t in ("INTEGER", "LONG", "SHORT"):
            return int
        if t in ("FLOAT", "DOUBLE"):
            return float
        if t == "BOOLEAN":
            return bool
        return str

    @staticmethod
    def _make_key(comp_id: str, chan_id: str) -> str:
        """Build a valid, lowercase lories channel key from an OpenEMS address."""
        raw = f"{comp_id.lstrip('_')}_{chan_id}"
        return re.sub(r"[^a-zA-Z0-9]+", "_", raw).strip("_").lower()
