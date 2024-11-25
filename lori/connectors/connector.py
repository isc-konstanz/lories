# -*- coding: utf-8 -*-
"""
lori.connectors.connector
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from collections import OrderedDict
from functools import wraps
from typing import Any, Dict, List, Optional

import pandas as pd
import pytz as tz
from lori import Channel, Channels, ChannelState
from lori.core import Context, Registrator, Resource, ResourceException, Resources, ResourceUnavailableException
from lori.core.configs import ConfigurationException, Configurations, Configurator, ConfiguratorMeta


class ConnectorMeta(ConfiguratorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        connector = super().__call__(*args, **kwargs)

        connector._Connector__connect = connector.connect
        connector.connect = connector._do_connect

        connector._Connector__disconnect = connector.disconnect
        connector.disconnect = connector._do_disconnect

        return connector


class Connector(Registrator, metaclass=ConnectorMeta):
    SECTION: str = "connector"
    SECTIONS: List[str] = []

    _connected: bool = False
    _connect_timestamp: pd.Timestamp = pd.NaT
    _disconnect_timestamp: pd.Timestamp = pd.NaT
    _reconnect_interval: pd.Timedelta = pd.Timedelta(minutes=1)

    __resources: Resources

    def __init__(
        self,
        context: Registrator | Context,
        configs: Optional[Configurations] = None,
        **kwargs,
    ) -> None:
        super().__init__(context=context, configs=configs, **kwargs)
        self.__resources = Resources()

    def __enter__(self) -> Connector:
        self.connect(self.__resources)
        return self

    # noinspection PyShadowingBuiltins
    def __exit__(self, type, value, traceback):
        self.disconnect()

    @classmethod
    def _assert_context(cls, context: Registrator | Context) -> Context:
        if context is None:
            raise ResourceException(f"Invalid '{cls.__name__}' context: {type(context)}")
        return super()._assert_context(context)

    # noinspection PyShadowingBuiltins, PyProtectedMember
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()

        # Channels are a subset of resources, hence omit them from printing
        vars.pop("channels", None)
        return vars

    # noinspection PyShadowingBuiltins
    def _convert_vars(self, convert: callable = str) -> Dict[str, str]:
        vars = self._get_vars()
        values = OrderedDict()
        try:
            id = vars.pop("id", self.id)
            key = vars.pop("key", self.key)
            if id != key:
                values["id"] = id
            values["key"] = key
        except (ResourceException, AttributeError):
            # Abstract properties are not yet instanced
            pass

        if "name" in vars:
            values["name"] = vars.pop("name")

        values.update(
            {
                k: str(v) if not isinstance(v, (Resource, Resources, Configurator, Context)) else convert(v)
                for k, v in vars.items()
            }
        )
        values["context"] = convert(self.context)
        values["configurations"] = convert(self.configs)
        values["configured"] = str(self.is_configured())
        values["connected"] = str(self._is_connected())
        values["enabled"] = str(self.is_enabled())
        return values

    @property
    def resources(self) -> Resources:
        return self.__resources

    @property
    def channels(self) -> Channels:
        return Channels([resource for resource in self.__resources if isinstance(resource, Channel)])

    def set_channels(self, state: ChannelState) -> None:
        # Set only channel states for channels, that actively are getting read or written by this connector.
        # Local channels may be logging channels as well, which need to be skipped.
        for channel in self.channels.filter(lambda c: c.has_connector(self.id)):
            channel.state = state

    def _is_disconnected(self) -> bool:
        return not self._is_connected()

    def _is_reconnectable(self) -> bool:
        return (
            self.is_enabled()
            and self.is_configured()
            and self._is_disconnected()
            and (
                pd.isna(self._disconnect_timestamp)
                or pd.Timestamp.now(tz.UTC) >= self._disconnect_timestamp + self._reconnect_interval
            )
        )

    def _is_connected(self) -> bool:
        return self.is_connected() and self._connected

    def is_connected(self) -> bool:
        return True

    def connect(self, resources: Resources) -> None:
        pass

    # noinspection PyUnresolvedReferences, PyTypeChecker
    @wraps(connect, updated=())
    def _do_connect(self, resources: Resources, *args, **kwargs) -> None:
        if not self.is_enabled():
            raise ConfigurationException(f"Trying to connect disabled {type(self).__name__}: {self.id}")
        if not self.is_configured():
            raise ConfigurationException(f"Trying to connect unconfigured {type(self).__name__}: {self.id}")
        if self._is_connected():
            self._logger.warning(f"{type(self).__name__} '{self.id}' already connected")
            return

        self.__connect(resources, *args, **kwargs)
        self._on_connect(resources)
        self._disconnect_timestamp = pd.NaT
        self._connect_timestamp = pd.Timestamp.now(tz.UTC)
        self._connected = True
        self.__resources = resources

    def _on_connect(self, resources: Resources) -> None:
        pass

    def disconnect(self) -> None:
        pass

    # noinspection PyUnresolvedReferences, PyTypeChecker
    @wraps(disconnect, updated=())
    def _do_disconnect(self) -> None:
        if self._is_connected():
            return

        self.__disconnect()
        self._on_disconnect()
        self._disconnect_timestamp = pd.Timestamp.now(tz.UTC)
        self._connect_timestamp = pd.NaT
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
    def __init__(self, connector: Connector, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.connector = connector


class ConnectorUnavailableException(ResourceUnavailableException, ConnectorException):
    """
    Raise if an accessed connector can not be found.

    """


class ConnectionException(ConnectorException):
    """
    Raise if an error occurred with the connection.

    """
