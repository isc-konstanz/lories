# -*- coding: utf-8 -*-
"""
lories.connectors.opcua
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Dict, Optional

import opcua

import pandas as pd
import pytz as tz
from lories._core import ChannelState  # noqa
from lories.connectors import Connector, register_connector_type
from lories.core import ConfigurationError
from lories.data import Channel
from lories.typing import Configurations, Resources


@register_connector_type("opc", "opcua")
class OpcUaConnector(Connector):

    _host: str
    _port: int
    _timeout: int
    _settings: str

    _client: Optional[opcua.Client]
    _nodes: Dict[str, opcua.Node]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)


        self._host = configs.get("host", default="127.0.0.1")
        self._port = configs.get_int("port", default=4840)
        self._timeout = configs.get_int("timeout", default=60)
        self._settings = [s.strip() for s in configs.get("settings", default="").split(",")]
        #self._endpoint = configs.get("endpoint", default="")
        
        self._client = opcua.Client(
            f"opc.tcp://{self._host}:{self._port}",
            timeout=self._timeout
        )
    
        if "username" in configs and "password" in configs:
            self._client.set_user(configs.get("username"))
            self._client.set_password(configs.get("password"))
            
        self._nodes = {}



    def is_connected(self) -> bool:
        return self._client is not None \
            and True
            # self._client.uaclient._uasocket is not None and \
            # self._client.uaclient._uasocket.connected

    def connect(self, resources: Resources) -> None:
        self._client.connect()

        # Todo: is filtering needed here?
        channels = resources.filter(lambda r: isinstance(r, Channel))
        for channel in channels:
            try:
                address = channel.get("address", channel.id)
                node_name = ";".join([*self._settings.split(","), f"s={address}"])
                node = self._client.get_node(node_name.strip())
                self._nodes[channel.id] = node
            except Exception as e:
                self._logger.warning(f"Failed to get OPC UA node for channel '{channel.id}': {e}")
        
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
                self._logger.warning(f"Node for channel '{channel.id}' not found")
                data.at[timestamp, channel.id] = ChannelState.NOT_AVAILABLE
                continue
            
            try:
                value = node.get_value()
                data.at[timestamp, channel.id] = value
            except Exception as e:
                self._logger.warning(f"Failed to read value for channel '{channel.id}': {e}")
                data.at[timestamp, channel.id] = ChannelState.NOT_AVAILABLE
        return data

    def write(self, data: pd.DataFrame) -> None:
        for channel in self.channels:
            node = self._nodes.get(channel.id)
            if node is None:
                self._logger.warning(f"Node for channel '{channel.id}' not found")
                continue
            
            try:
                value = data.at[data.index[-1], channel.id]
                node.set_value(value)
            except Exception as e:
                self._logger.warning(f"Failed to write value for channel '{channel.id}': {e}")
    
