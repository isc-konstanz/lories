# -*- coding: utf-8 -*-
"""
    loris.core.channels.channel
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Optional, Any

import pandas as pd
import logging

from collections import OrderedDict
from loris.util import parse_id
from loris.core.channels import ChannelState, ChannelConnector

logger = logging.getLogger(__name__)


class Channel:

    _uuid: str

    reader: ChannelConnector
    writer: ChannelConnector

    _configs: OrderedDict[str, Any]

    _time: pd.Timestamp = pd.NaT
    _value: Optional[Any] = None
    _state: str | ChannelState = ChannelState.DISABLED

    # noinspection PyShadowingBuiltins
    def __init__(self, uuid: str = None, id: str = None, **configs: Any) -> None:
        if id is None:
            from loris import ConfigurationException
            raise ConfigurationException('Invalid configuration, missing specified channel ID')

        self.reader = ChannelConnector('reader', **configs.pop('reader', {}))
        self.writer = ChannelConnector('writer', **configs.pop('writer', {}))

        self._configs = OrderedDict({'id': parse_id(id), **configs})
        if self._configs['id'] != id:
            logger.warning(f'Value container ID contains invalid characters: {id}')

        self._uuid = uuid if uuid is not None else self.id

    def __repr__(self) -> str:
        representation = 'Channel:\n\t' + '\n\t'.join(f'{key}: {str(val)}' for key, val in self._configs.items())
        return representation + f'\n\ttime: {str(self.time)}' + \
                                f'\n\tvalue: {str(self.value)}' + \
                                f'\n\tstatus: {str(self.state)}'

    def __contains__(self, attr):
        return attr in ['id', 'time', 'value', 'state', 'reader', 'writer'] + list(self._configs.keys())

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = Channel.__getattribute__(self, '_configs')
        if attr in configs.keys():
            return configs[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no configuration '{attr}'")

    @property
    def id(self) -> str:
        return self._configs['id']

    @property
    def time(self) -> Optional[pd.Timestamp, pd.NaT]:
        return self._time

    @property
    def value(self) -> Optional[Any]:
        return self._value

    @value.setter
    def value(self, value) -> None:
        self._value = value
        self._time = pd.Timestamp.now()
        self._state = ChannelState.VALID

    @property
    def state(self) -> ChannelState | str:
        return self._state

    @state.setter
    def state(self, state) -> None:
        self._value = None
        self._time = pd.Timestamp.now()
        self._state = state

    def set(self, time: pd.Timestamp, value: Any, state: str | ChannelState = ChannelState.VALID) -> None:
        self._time = time
        self._value = value
        self._state = state

    # noinspection PyProtectedMember
    def has_reader(self, *uuids) -> bool:
        return any(self.reader._uuid == uuid for uuid in uuids) if len(uuids) > 0 else self.reader._uuid is not None

    # noinspection PyProtectedMember
    def has_writer(self, *uuids) -> bool:
        return any(self.writer._uuid == uuid for uuid in uuids) if len(uuids) > 0 else self.writer._uuid is not None
