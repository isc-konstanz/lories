# -*- coding: utf-8 -*-
"""
loris.connectors.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd
from loris import Channels, ChannelState, Configurable, ConfigurationException, Configurations, LocalResourceException
from loris.util import parse_id


class Connector(ABC, Configurable):
    _uuid: str
    _id: str

    _channels: Channels

    _connect_timestamp: pd.Timestamp = pd.NaT
    _disconnect_timestamp: pd.Timestamp = pd.NaT

    def __init__(self, context, configs: Configurations, channels: Channels = None, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        if "id" not in configs:
            raise ConfigurationException("Invalid configuration, missing specified connector ID")

        self._id = parse_id(configs.get("id"))
        self._uuid = configs.pop("uuid") if "uuid" in configs else self.id
        self._context = context
        self._channels = channels if channels is not None else Channels()

    def __enter__(self) -> Connector:
        self.connect(self._channels)
        return self

    # noinspection PyShadowingBuiltins
    def __exit__(self, type, value, traceback):
        self.disconnect()

    def __connect__(self, channels: Channels) -> None:
        pass

    def __disconnect__(self) -> None:
        pass

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def id(self) -> str:
        return self._id

    def set_states(self, channel_state: ChannelState) -> None:
        for channel in self._channels:
            channel.state = channel_state

    def connect(self, channels: Channels) -> None:
        self._channels = channels
        self._connect_timestamp = pd.Timestamp.now()
        self.__connect__(channels)

    def disconnect(self) -> None:
        self._disconnect_timestamp = pd.Timestamp.now()
        self.__disconnect__()

    @abstractmethod
    def read(
        self,
        channels: Channels,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> None:
        pass

    @abstractmethod
    def write(self, channels: Channels) -> None:
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        pass


class ConnectorException(LocalResourceException):
    """
    Raise if an error occurred accessing the connector.

    """

    # noinspection PyArgumentList
    def __init__(self, *args, connector: Optional[Connector] = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.connector = connector


class ConnectionException(ConnectorException, IOError):
    """
    Raise if an error occurred with the connection.

    """
