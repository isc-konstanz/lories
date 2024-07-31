# -*- coding: utf-8 -*-
"""
loris.connectors.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from functools import wraps
from typing import Any, Dict, List, Optional

import pandas as pd
import pytz as tz
from loris import (
    Channel,
    Channels,
    ChannelState,
    ConfigurationException,
    Configurations,
    Configurator,
    Context,
    Resource,
    ResourceException,
    Resources,
)
from loris.core import ConfiguratorMeta
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

    __resources: Resources

    def __init__(self, context: Context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(get_context(context, Context), configs, *args, **kwargs)
        from loris.components.activator import Activator
        from loris.connectors.context import ConnectorContext
        from loris.data.context import DataContext

        if context is None or not isinstance(context, (ConnectorContext, DataContext)):
            raise ConnectorException(f"Invalid connector context: {None if context is None else type(context)}")
        if configs is None:
            raise ConfigurationException("Invalid connector for empty configuration")

        if "id" not in configs:
            raise ConfigurationException("Invalid configuration, missing specified connector ID")

        self._id = parse_id(configs.get("id"))
        if "uuid" in configs:
            self._uuid = configs.pop("uuid")
        else:
            self._uuid = self._id if not isinstance(context, Activator) else f"{context.uuid}.{self._id}"

        self.__resources = Resources()

    def __enter__(self) -> Connector:
        self._do_connect(self.__resources)
        return self

    # noinspection PyShadowingBuiltins
    def __exit__(self, type, value, traceback):
        self._do_disconnect()

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()
        vars.pop("type", None)
        return vars

    # noinspection PyShadowingBuiltins
    def _parse_vars(self, vars: Optional[Dict[str, Any]] = None, parse: callable = str) -> List[str]:
        if vars is None:
            vars = self._get_vars()
        values = []

        uuid = vars.pop("uuid", self.uuid)
        id = vars.pop("id", self.id)
        if uuid != id:
            values.append(f"uuid={uuid}")
        values.append(f"id={id}")

        if "name" in vars:
            values.append(f"name={vars.pop('name')}")

        values += [f"{k}={v if not isinstance(v, (Resource, Configurator)) else parse(v)}" for k, v in vars.items()]

        values.append(f"context={parse(self.context)}")
        values.append(f"configurations={repr(self.configs)}")
        values.append(f"configured={self.is_configured()}")
        values.append(f"connected={self._is_connected()}")
        values.append(f"enabled={self.is_enabled()}")
        return values

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
    def resources(self) -> Resources:
        return self.__resources

    @property
    def channels(self) -> Channels:
        return Channels([resource for resource in self.__resources if isinstance(resource, Channel)])

    def set_channels(self, state: ChannelState) -> None:
        for channel in self.channels:
            channel.state = state

    def _is_connected(self) -> bool:
        return self.is_connected() and self._connected

    def is_connected(self) -> bool:
        return True

    def connect(self, resources: Resources) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(connect, updated=())
    def _do_connect(self, resources: Resources) -> None:
        if not self.is_enabled():
            raise ConfigurationException(f"Trying to connect disabled {type(self).__name__}: {self.uuid}")
        if not self.is_configured():
            raise ConfigurationException(f"Trying to connect unconfigured {type(self).__name__}: {self.uuid}")
        if self._is_connected():
            self._logger.warning(f"{type(self).__name__} '{self.uuid}' already connected")
            return

        self.__connect(resources)
        self._on_connect(resources)
        self._connect_timestamp = pd.Timestamp.now(tz.UTC)
        self._connected = True
        self.__resources = resources

    def _on_connect(self, resources: Resources) -> None:
        pass

    def disconnect(self) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(disconnect, updated=())
    def _do_disconnect(self) -> None:
        if self._is_connected():
            return

        self.__disconnect()
        self._on_disconnect()
        self._disconnect_timestamp = pd.Timestamp.now(tz.UTC)
        self._connected = False

    def _on_disconnect(self) -> None:
        pass

    @abstractmethod
    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(self, data: pd.DataFrame) -> None:
        pass


class ConnectorException(ResourceException):
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
