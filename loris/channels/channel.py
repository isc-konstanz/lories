# -*- coding: utf-8 -*-
"""
    loris.channels.channel
    ~~~~~~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Optional, Any

import pandas as pd
import logging

from copy import deepcopy
from collections import OrderedDict
from loris.configs import ConfigurationException
from loris.channels import ChannelState, ChannelConnector
from loris.util import parse_id, _parse_freq, to_timedelta

logger = logging.getLogger(__name__)


class Channel:

    _uuid: str

    logger: ChannelConnector
    connector: ChannelConnector

    _configs: OrderedDict[str, Any]

    _timestamp: pd.Timestamp = pd.NaT
    _value: Optional[Any] = None
    _state: str | ChannelState = ChannelState.DISABLED

    # noinspection PyShadowingBuiltins
    def __init__(self, uuid: str = None, id: str = None, **configs: Any) -> None:
        if id is None:
            raise ConfigurationException('Invalid configuration, missing specified channel ID')

        _connector = configs.pop('connector', {})
        if isinstance(_connector, str):
            _connector = {'connector': _connector}
        self.connector = ChannelConnector(**_connector)

        _logger = configs.pop('logger', {})
        if isinstance(_logger, str):
            _logger = {'connector': _logger}
        self.logger = ChannelConnector(**_logger)

        self._configs = OrderedDict({'id': parse_id(id), **configs})
        if self._configs['id'] != id:
            logger.warning(f'Value container ID contains invalid characters: {id}')

        self._uuid = uuid if uuid is not None else self.id

    def __repr__(self) -> str:
        representation = 'Channel:\n\t' + '\n\t'.join(f'{key}: {str(val)}' for key, val in self._configs.items())
        return representation + f'\n\ttimestamp: {str(self.timestamp)}' + \
                                f'\n\tvalue: {str(self.value)}' + \
                                f'\n\tstatus: {str(self.state)}'

    def __contains__(self, attr):
        return attr in ['id', 'timestamp', 'value', 'state', 'logger', 'connector'] + list(self._configs.keys())

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = Channel.__getattribute__(self, '_configs')
        if attr in configs.keys():
            return configs[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no configuration '{attr}'")

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def id(self) -> str:
        return self._configs['id']

    @property
    def freq(self) -> Optional[str]:
        for k in ['freq', 'frequency', 'resolution']:
            if k in self._configs:
                return _parse_freq(self._configs[k])
        return None

    @property
    def timedelta(self) -> Optional[pd.Timedelta]:
        return to_timedelta(self.freq)

    @property
    def timestamp(self) -> pd.Timestamp | pd.NaT:
        return self._timestamp

    @property
    def value(self) -> Optional[Any]:
        return self._value

    @value.setter
    def value(self, value) -> None:
        self._value = value
        self._timestamp = pd.Timestamp.now()
        self._state = ChannelState.VALID

    @property
    def state(self) -> ChannelState | str:
        return self._state

    @state.setter
    def state(self, state) -> None:
        self._value = None
        self._timestamp = pd.Timestamp.now()
        self._state = state

    def set(self, timestamp: pd.Timestamp, value: Any, state: str | ChannelState = ChannelState.VALID) -> None:
        self._timestamp = timestamp
        self._value = value
        self._state = state

    def copy(self) -> Channel:
        configs = deepcopy(self._configs)
        configs['logger'] = self.logger.copy()
        configs['connector'] = self.connector.copy()
        return Channel(self.id, self._uuid, **configs)

    # noinspection PyProtectedMember
    def has_logger(self, *uuids: Optional[str]) -> bool:
        return any(self.logger._uuid == uuid for uuid in uuids) if len(uuids) > 0 else self.logger._uuid is not None

    # noinspection PyProtectedMember
    def has_connector(self, uuid: Optional[str] = None) -> bool:
        return self.connector._uuid == uuid if uuid is not None else self.connector._uuid is not None
