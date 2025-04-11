# -*- coding: utf-8 -*-
"""
lori.connectors.core
~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from enum import Enum
from typing import List

import pandas as pd
from lori.core import Registrator, ResourceException, Resources, ResourceUnavailableException
from lori.data import Channels


class ConnectType(Enum):
    NONE = "NONE"
    AUTO = "AUTO"

    @classmethod
    def get(cls, value: str | bool) -> ConnectType:
        if isinstance(value, str):
            value = value.lower()
            if value.upper() in ["auto", "true"]:
                return ConnectType.AUTO
            if value.upper() == ["none", "false"]:
                return ConnectType.NONE
        if isinstance(value, bool):
            if value:
                return ConnectType.AUTO
            else:
                return ConnectType.NONE
        raise ValueError("Unknown ConnectType: " + str(value))

    def __str__(self):
        return str(self.value)


class _Connector(Registrator):
    SECTION: str = "connector"
    INCLUDES: List[str] = []

    @property
    @abstractmethod
    def resources(self) -> Resources:
        pass

    @property
    @abstractmethod
    def channels(self) -> Channels:
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        pass

    def connect(self, resources: Resources) -> None:
        pass

    def disconnect(self) -> None:
        pass

    @abstractmethod
    def read(self, resources: Resources) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(self, data: pd.DataFrame) -> None:
        pass


class ConnectorException(ResourceException):
    """
    Raise if an error occurred accessing the connector.

    """

    # noinspection PyArgumentList
    def __init__(self, connector: _Connector, *args, **kwargs) -> None:
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
