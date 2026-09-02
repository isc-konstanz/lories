# -*- coding: utf-8 -*-
"""
lories.components.openems
~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

import fnmatch
import json
import re
import time
from typing import Optional

from lories.components import Component, register_component_type
from lories.connectors import registry as connector_registry
from lories.connectors.openems import OpenEMSConnector
from lories.core.configs.errors import ConfigurationError
from lories.core.configs.parameters import ConnectorParameter, ListParameter
from lories.io.rest import Rest
from lories.typing import Configurations


@register_component_type("openems")
class OpenEMSComponent(Component):
    """A component that creates channels for an explicit allowlist of OpenEMS
    channel addresses, discovered and validated via one REST call at configure
    time.

    Only addresses matching a configured pattern are created — there is no
    mirror-everything behaviour and no ``_``-prefix hiding; addresses such as
    ``_sum/GridActivePower`` must be named explicitly to be exposed.

    The concrete WebSocket connector is selected via the ``type`` key in the
    ``[connector]`` sub-section and is always created internally — no separate
    ``[connectors.openems]`` entry is needed in ``system.conf``.

    Example ``system.conf`` (Edge device)::

        [components.openems]
        type = "openems"
        channels = ["_sum/Grid*", "ess0/Soc"]

        [components.openems.connector]
        type          = "openems_edge"
        host          = "localhost"
        username      = "admin"
        password      = "admin"
        timeout       = 10
        rest_port     = 8084
        rest_endpoint = "rest"
        ws_port       = 8085
    """

    _channels = ListParameter(
        key="channels",
        item_type=str,
        desc="OpenEMS channel address patterns (fnmatch glob, e.g. '_sum/Grid*') to create channels for",
    )
    _connector = ConnectorParameter(
        cls=OpenEMSConnector,
        key="connector",
        desc="OpenEMS WebSocket connector (edge or backend) backing this component",
    )

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        connector_configs = configs.get_member("connector", defaults={})
        connector_type = connector_configs.get("type", default="openems_edge")
        if not connector_registry.has_type(connector_type):
            raise ConfigurationError(f"Unknown OpenEMS connector type '{connector_type}'")

        connector_cls = connector_registry.from_type(connector_type).type
        if not issubclass(connector_cls, OpenEMSConnector):
            raise ConfigurationError(
                f"Connector type '{connector_type}' ('{connector_cls.__name__}') is not an OpenEMSConnector"
            )

        connector = connector_cls(
            key="openems",
            name=f"{self.name} OpenEMS",
            context=self,
            configs=connector_configs,
        )
        self.connectors.add(connector)

        rest = Rest(
            host=connector_configs.get("host", default="localhost"),
            port=connector_configs.get_int("rest_port", default=8084),
            username=connector_configs.get("username", default="admin"),
            password=connector_configs.get("password", default="admin"),
            endpoint=connector_configs.get("rest_endpoint", default="rest"),
            timeout=connector_configs.get_int("timeout", default=10),
        )

        raw = None
        error: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                raw = json.loads(rest.get_request("channel/.*/.*"))
                break
            except Exception as e:
                error = e
                self._logger.warning(f"OpenEMS REST channel discovery attempt {attempt}/3 failed: {e}")
                if attempt < 3:
                    time.sleep(attempt)
        if raw is None:
            raise ConfigurationError(f"OpenEMS REST channel discovery failed after 3 attempts: {error}")

        discovered = {channel["address"]: channel for channel in raw}

        matched = {}
        for pattern in self._channels:
            addresses = sorted(address for address in discovered if fnmatch.fnmatchcase(address, pattern))
            if not addresses:
                raise ConfigurationError(f"OpenEMS channel pattern '{pattern}' matched no discovered channels")
            for address in addresses:
                matched[address] = discovered[address]

        for address in sorted(matched):
            channel = matched[address]
            comp_id, chan_id = address.split("/", 1)
            self.data.add(
                key=self._make_key(comp_id, chan_id),
                name=f"{comp_id} / {chan_id}",
                type=self._map_type(channel.get("type")),
                address=address,
                unit=channel.get("unit") or None,
                aggregate="last",
                connector=connector.id,
            )

        self._logger.info(f"Created {len(matched)} OpenEMS channels")
        self._logger.debug(f"OpenEMS channels: {sorted(matched)}")

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
