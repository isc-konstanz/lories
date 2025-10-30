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
        self._settings = configs.get("settings", default=None)
        
        self._client = opcua.Client(
            f"opc.tcp://{self._host}:{self._port}",
            timeout=self._timeout*1000
        )
        self._nodes = {}
    
        
        if "username" in configs and "password" in configs:
            self._client.set_user(configs.get("username"))
            self._client.set_password(configs.get("password"))
            
        # elif ...
        #     self._client.tls_set(
        #         ca_certs=configs.get("ca_certs", default=None),
        #         certfile=configs.get("certfile", default=None),
        #         keyfile=configs.get("keyfile", default=None),
        #         cert_reqs=configs.get("cert_reqs", default=None),
        #         tls_version=configs.get("tls_version", default=None),
        #         ciphers=configs.get("ciphers", default=None),
        #         keyfile_password=configs.get("keyfile_password", default=None),
        #         alpn_protocols=configs.get("alpn_protocols", default=None),
        #     )


    def is_connected(self) -> bool:
        return self._client is not None \
            and True
            # self._client.uaclient._uasocket is not None and \
            # self._client.uaclient._uasocket.connected

    def connect(self, resources: Resources) -> None:
        self._client.connect()

        channels = resources.filter(lambda r: isinstance(r, Channel))
        for channel in channels:
            try:
                root = self._client.get_root_node()

                # Recursive search function
                def find_node_by_name(node, name):
                    for child in node.get_children():
                        bname = child.get_browse_name().Name
                        if bname == name:
                            return child
                        found = find_node_by_name(child, name)
                        if found:
                            return found
                    return None

                self._nodes[channel.id] = find_node_by_name(root, channel.key)
            except Exception as e:
                self._logger.warning(f"Failed to get OPC UA node for channel '{channel.id}': {e}")
        
        

    def disconnect(self) -> None:
        if self.is_connected():
            self._client.disconnect()


    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz.UTC).floor(freq="s")
        data = pd.DataFrame(index=[timestamp], columns=resources.ids)
        for channel in resources.filter(lambda r: isinstance(r, Channel)):
            node = self._nodes.get(channel.id)
            if node is not None:
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
            if node is not None and channel.id in data.columns:
                try:
                    value = data.at[data.index[-1], channel.id]
                    node.set_value(value)
                except Exception as e:
                    self._logger.warning(f"Failed to write value for channel '{channel.id}': {e}")
    
