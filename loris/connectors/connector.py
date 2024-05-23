# -*- coding: utf-8 -*-
"""
    loris.connectors.connector
    ~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Optional
from abc import ABC, abstractmethod

import pandas as pd
import datetime as dt

from loris import Channels, ChannelState, Configurable, Configurations, ConfigurationException, LocalResourceException
from loris.util import parse_id


class Connector(ABC, Configurable):

    _uuid: str
    _id: str

    def __init__(self, context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        if 'id' not in configs:
            raise ConfigurationException('Invalid configuration, missing specified connector ID')

        self._id = parse_id(configs.get('id'))
        self._uuid = configs.pop('uuid') if 'uuid' in configs else self.id
        self._context = context

    @property
    def id(self) -> str:
        return self._id

    @abstractmethod
    def connect(self, channels: Channels) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    def read(self,
             channels: Channels,
             start: Optional[pd.Timestamp, dt.datetime] = None,
             end:   Optional[pd.Timestamp, dt.datetime] = None) -> None:
        raise NotImplementedError

    def write(self, channels: Channels) -> None:
        raise NotImplementedError

    @abstractmethod
    def is_connected(self) -> bool:
        pass


class ConnectorException(LocalResourceException):
    """
    Raise if an error occurred accessing the connector.

    """

    # noinspection PyArgumentList
    def __init__(self, connector: Connector, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.connector = connector


class ConnectionException(ConnectorException, IOError):
    """
    Raise if an error occurred with the connection.

    """

