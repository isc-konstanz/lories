# -*- coding: utf-8 -*-
"""
lories.connectors.opcua
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import math
from typing import Dict, List, Optional

import opcua

import pandas as pd
import pytz as tz
from lories._core import ChannelState  # noqa
from lories.connectors import Connector, register_connector_type
from lories.core.configs.parameters import ChannelParameter, Parameter
from lories.data import Channel
from lories.typing import Configurations, Resources


@register_connector_type("opc", "opcua")
class OpcUaConnector(Connector):
    """
    OPC UA (Open Platform Communications Unified Architecture) is an industrial automation standard that
    facilitates secure, real-time data exchange between devices and systems. It offers robust security features,
    interoperability across different platforms, and scalability for large-scale deployments. However, OPC UA can
    be more complex to set up and configure, may involve additional licensing costs, and requires a learning curve
    for users familiarizing themselves with its standards and best practices.
    """

    _host = Parameter(key="host", type=str, default="127.0.0.1", desc="OPC UA server host")
    _port = Parameter(key="port", type=int, default=4840, min=1, max=65535, desc="OPC UA server port")
    _timeout = Parameter(key="timeout", type=int, default=60, desc="Connection timeout (s)")
    _settings = Parameter(
        key="settings", type=List[str], default=[], desc="Extra OPC UA node-id prefixes (e.g. 'ns=2')"
    )
    _username = Parameter(key="username", type=str, required=False, desc="Optional username for authentication")
    _password = Parameter(key="password", type=str, required=False, desc="Optional password for authentication")

    # Per-channel parameters
    address = ChannelParameter(type=str, required=False, desc="OPC UA node string identifier (defaults to channel id)")

    _client: Optional[opcua.Client]
    _nodes: Dict[str, opcua.Node]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = None
        self._nodes = {}

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self._client = opcua.Client(
            f"opc.tcp://{self._host}:{self._port}",
            timeout=self._timeout,
        )

        if self._username and self._password:
            self._client.set_user(self._username)
            self._client.set_password(self._password)
        elif self._username or self._password:
            self._logger.warning("Only one of 'username' or 'password' provided. Both are required for authentication.")

        # Disable all log messages from the 'opcua' logger
        logging.getLogger("opcua").setLevel(logging.CRITICAL)

    def is_connected(self) -> bool:
        return self._client is not None

    def connect(self, resources: Resources) -> None:
        self._client.connect()

        # Todo: is filtering needed here?
        channels = resources.filter(lambda r: isinstance(r, Channel))
        for channel in channels:
            try:
                address = channel.get("address", channel.id)
                node_name = ";".join([*self._settings, f"s={address}"])
                node = self._client.get_node(node_name.strip())
                self._nodes[channel.id] = node
            except Exception as e:
                self._logger.warning(f"Failed to get OPC UA node for '{channel.id}': {e}")

    def disconnect(self) -> None:
        if self.is_connected():
            self._client.disconnect()
            self._client = None
            self._nodes = {}

    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz.UTC).floor(freq="s")
        data = pd.DataFrame(index=[timestamp], columns=resources.ids)

        for channel in resources:
            node = self._nodes.get(channel.id)
            if node is None:
                self._logger.warning(f"Node for '{channel.id}' not found")
                data.at[timestamp, channel.id] = ChannelState.NOT_AVAILABLE
                continue

            try:
                value = node.get_value()
                if math.isnan(value):
                    value = ChannelState.NOT_AVAILABLE
                data.at[timestamp, channel.id] = value
            except Exception as e:
                data.at[timestamp, channel.id] = ChannelState.NOT_AVAILABLE
                self._logger.warning(f"Failed to read value for '{channel.id}': {e}")
        return data

    def write(self, data: pd.DataFrame) -> None:
        for channel in self.channels:
            node = self._nodes.get(channel.id)
            if node is None:
                self._logger.warning(f"Node for '{channel.id}' not found")
            else:
                try:
                    value = data.at[data.index[-1], channel.id]
                    node.set_value(value)
                except Exception as e:
                    self._logger.warning(f"Failed to write value for '{channel.id}': {e}")
