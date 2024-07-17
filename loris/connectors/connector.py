# -*- coding: utf-8 -*-
"""
loris.connectors.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from functools import wraps
from typing import Optional

import pandas as pd
import pytz as tz
from loris import Channels, ChannelState, ConfigurationException, Configurations, Configurator, LocalResourceException
from loris.configs import ConfiguratorMeta
from loris.util import get_context, parse_id


class ConnectorMeta(ConfiguratorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        connector = super().__call__(*args, **kwargs)

        connector._Connector__connect = connector.connect
        connector.connect = connector._do_connect

        connector._Connector__disconnect = connector.disconnect
        connector.disconnect = connector._do_disconnect

        return connector


class Connector(Configurator, metaclass=ConnectorMeta):
    _uuid: str
    _id: str

    _connected: bool = False
    _connect_timestamp: pd.Timestamp = pd.NaT
    _disconnect_timestamp: pd.Timestamp = pd.NaT
    _reconnect_interval: pd.Timedelta = pd.Timedelta(minutes=1)

    __channels: Channels = None

    def __init__(self, context, configs: Configurations, channels: Channels = None, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        if "id" not in configs:
            raise ConfigurationException("Invalid configuration, missing specified connector ID")

        self._id = parse_id(configs.get("id"))
        self._uuid = configs.pop("uuid") if "uuid" in configs else self._id

        from loris.data.context import DataContext
        from loris.connectors.context import ConnectorContext
        if not isinstance(context, (ConnectorContext, DataContext)):
            raise ConnectorException(f"Invalid connector context type: {type(context)}")

        self.__channels = channels if channels is not None else Channels()
        self.__context = get_context(context, (ConnectorContext, DataContext))

    def __enter__(self) -> Connector:
        self._do_connect(self.__channels)
        return self

    # noinspection PyShadowingBuiltins
    def __exit__(self, type, value, traceback):
        self._do_disconnect()

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def id(self) -> str:
        return self._id

    @property
    @abstractmethod
    def type(self) -> str:
        pass

    @property
    def context(self):
        return self.__context

    @property
    def channels(self) -> Channels:
        return self.__channels

    def set_states(self, channel_state: ChannelState) -> None:
        for channel in self.__channels:
            channel.state = channel_state

    def _is_connected(self) -> bool:
        return self.is_connected() and not pd.isna(self._connect_timestamp)

    @abstractmethod
    def is_connected(self) -> bool:
        pass

    @abstractmethod
    def connect(self, channels: Channels) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(connect, updated=())
    def _do_connect(self, channels: Channels) -> None:
        if not self.is_enabled():
            raise ConfigurationException(f"Trying to connect disabled {type(self).__name__}: {self.uuid}")
        if not self.is_configured():
            raise ConfigurationException(f"Trying to connect unconfigured {type(self).__name__}: {self.uuid}")
        if self._is_connected():
            self._logger.warning(f"{type(self).__name__} '{self.uuid}' already connected")
            return
        self._logger.info(f"Connecting {type(self).__name__}: {self.uuid}")

        self.__connect(channels)
        self._on_connect(channels)
        self._connect_timestamp = pd.Timestamp.now(tz.UTC)
        self.__channels = channels

        self._logger.debug(f"Connected {type(self).__name__}: {self.uuid}")

    def _on_connect(self, channels: Channels) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(disconnect, updated=())
    def _do_disconnect(self) -> None:
        if self._is_connected():
            return
        self._logger.info(f"Disconnecting {type(self).__name__}: {self.uuid}")

        self.__disconnect()
        self._on_disconnect()
        self._disconnect_timestamp = pd.Timestamp.now(tz.UTC)

        self._logger.debug(f"Disconnected {type(self).__name__}: {self.uuid}")

    def _on_disconnect(self) -> None:
        pass

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
