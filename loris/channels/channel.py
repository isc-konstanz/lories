# -*- coding: utf-8 -*-
"""
loris.channels.channel
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from collections import OrderedDict
from copy import deepcopy
from pydoc import locate
from typing import Any, Optional, Type

import pandas as pd
import pytz as tz
from loris.channels import ChannelConnector, ChannelState
from loris.configs import ConfigurationException
from loris.util import _parse_freq, parse_id, to_timedelta


class Channel:
    _uuid: str
    _id: str

    name: str
    logger: ChannelConnector
    connector: ChannelConnector

    _configs: OrderedDict[str, Any]

    _timestamp: pd.Timestamp = pd.NaT
    _value: Optional[Any] = None
    _state: str | ChannelState = ChannelState.DISABLED

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        uuid: str = None,
        id: str = None,
        name: Optional[str] = None,
        value_type: Optional[str | Type] = None,
        value_length: Optional[int] = None,
        **configs: Any,
    ) -> None:
        self._logger = logging.getLogger(__name__)

        if id is None:
            raise ConfigurationException("Invalid configuration, missing specified channel ID")
        self._id = parse_id(id)
        self._uuid = uuid if uuid is not None else self._id
        if self._id != id:
            self._logger.warning(f"Value container ID contains invalid characters: {id}")

        if name is None:
            name = self.id
        self.name = name

        self._value_type = value_type
        self._value_length = value_length

        _connector = configs.pop("connector", {})
        if isinstance(_connector, ChannelConnector):
            self.connector = _connector
        else:
            if isinstance(_connector, str):
                _connector = {"connector": _connector}
            self.connector = ChannelConnector(**_connector)

        _logger = configs.pop("logger", {})
        if isinstance(_logger, ChannelConnector):
            self.logger = _logger
        else:
            if isinstance(_logger, str):
                _logger = {"connector": _logger}
            self.logger = ChannelConnector(**_logger)

        self._configs = OrderedDict(configs)

    def __repr__(self) -> str:
        return (
            "Channel:\n\t"
            + f"\n\tid: {self.id}"
            + f"\n\tname: {self.name}"
            + "\n\t".join(f"{key}: {str(val)}" for key, val in self._configs.items())
            + f"\n\tvalue type: {self.value_type}"
            + f"\n\tvalue length: {self._value_length}"
            + f"\n\tvalue: {str(self.value)}"
            + f"\n\tstatus: {str(self.state)}"
            + f"\n\ttimestamp: {str(self.timestamp)}"
        )

    def __contains__(self, attr):
        return attr in [
            "id",
            "name",
            "logger",
            "connector",
            "value_type",
            "value_length",
            "value",
            "state",
            "timestamp",
        ] + list(self._configs.keys())

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = Channel.__getattribute__(self, "_configs")
        if attr in configs.keys():
            return configs[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no configuration '{attr}'")

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def id(self) -> str:
        return self._id

    @property
    def freq(self) -> Optional[str]:
        for k in ["freq", "frequency", "resolution"]:
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
        self._set(pd.Timestamp.now(tz.UTC).floor(freq="s"), value, ChannelState.VALID)

    @property
    def value_type(self) -> Optional[Any]:
        if self._value_type is None:
            if self._value is None:
                return None
            type(self._value)
        if isinstance(self._value_type, str):
            return locate(self._value_type)
        return self._value_type

    @property
    def value_length(self) -> Optional[int]:
        if self._value_length is None:
            if self._value is None:
                return None
            len(self._value)
        return self._value_length

    @property
    def state(self) -> ChannelState | str:
        return self._state

    @state.setter
    def state(self, state) -> None:
        self._set(pd.Timestamp.now(tz.UTC).floor(freq="s"), None, state)

    def set(
        self,
        timestamp: pd.Timestamp,
        value: Any,
        state: Optional[str | ChannelState] = ChannelState.VALID
    ) -> None:
        self._set(timestamp, value, state)

    def _set(
        self,
        timestamp: pd.Timestamp,
        value: Optional[Any],
        state: str | ChannelState
    ) -> None:
        # TODO: Implement value type validation based on value type attribute
        self._timestamp = timestamp
        self._value = value
        self._state = state

    def copy(self) -> Channel:
        configs = deepcopy(self._configs)
        configs["name"] = self.name
        configs["logger"] = self.logger.copy()
        configs["connector"] = self.connector.copy()
        return Channel(uuid=self._uuid, id=self._id, name=self.name, **configs)

    # noinspection PyProtectedMember
    def from_logger(self) -> Channel:
        configs = deepcopy(self.logger._configs)
        configs["logger"] = deepcopy(self.logger)
        configs["connector"] = deepcopy(self.connector)
        channel = Channel(uuid=self._uuid, id=self._id, name=self.name, **configs)
        channel.set(self._timestamp, self._value, self._state)
        return channel

    def has_logger(self, *uuids: Optional[str]) -> bool:
        return any(self.logger.uuid == uuid for uuid in uuids) if len(uuids) > 0 else self.logger.uuid is not None

    def has_connector(self, uuid: Optional[str] = None) -> bool:
        return self.connector.uuid == uuid if uuid is not None else self.connector.uuid is not None

    def to_series(self, state: bool = False) -> pd.Series:
        if isinstance(self.value, pd.Series):
            return self.value
        elif isinstance(self.value, pd.DataFrame):
            return self.value[self.id]
        else:
            if state and pd.isna(self.value):
                data = self.state
            else:
                data = self.value
            return pd.Series(index=[self.timestamp], data=[data], name=self.id)
