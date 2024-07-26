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
from typing import Any, Dict, Optional, Type

import pandas as pd
import pytz as tz
from loris.channels import ChannelConnector, ChannelState
from loris.configs import ConfigurationException
from loris.util import _parse_freq, parse_id, to_timedelta


class Channel:
    _uuid: str
    _id: str
    _name: str

    logger: ChannelConnector
    connector: ChannelConnector

    __configs: OrderedDict[str, Any]

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
        self._name = name

        self._value_type = value_type
        self._value_length = value_length

        _connector = configs.pop("connector", {})
        if isinstance(_connector, ChannelConnector):
            self.connector = _connector
        else:
            if _connector is None or isinstance(_connector, str):
                _connector = {"connector": _connector}
            self.connector = ChannelConnector(**_connector)

        _logger = configs.pop("logger", {})
        if isinstance(_logger, ChannelConnector):
            self.logger = _logger
        else:
            if _logger is None or isinstance(_logger, str):
                _logger = {"connector": _logger}
            self.logger = ChannelConnector(**_logger)

        self.__configs = OrderedDict(configs)

    # noinspection PyShadowingBuiltins
    def __repr__(self) -> str:
        vars = OrderedDict({
            "id": self.id
        })
        if self.is_valid():
            vars["value"] = str(self.value)
        else:
            vars["state"] = str(self.state)
        vars["timestamp"] = str(self.timestamp)
        return f"{type(self).__name__}({', '.join(f'{k}={v}' for k, v in vars.items())})"

    # noinspection PyShadowingBuiltins
    def __str__(self) -> str:
        vars = OrderedDict({
            "id": self.id
        })
        if self.id != self.name:
            vars["name"] = self.name
        vars.update(self.__configs)
        vars["value type"] = self.value_type
        vars["value length"] = self._value_length
        vars["value"] = str(self.value)
        vars["state"] = str(self.status)
        vars["timestamp"] = str(self.timestamp)
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{k}={v}" for k, v in vars.items())

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
        ] + list(self.__configs.keys())

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = Channel.__getattribute__(self, f"_{Channel.__name__}__configs")
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
    def name(self) -> str:
        return self._name

    @property
    def freq(self) -> Optional[str]:
        for k in ["freq", "frequency", "resolution"]:
            if k in self.__configs:
                return _parse_freq(self.__configs[k])
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

    def is_valid(self):
        return self.state == ChannelState.VALID

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
        configs = self._copy_configs()
        configs["name"] = self.name
        configs["logger"] = self.logger.copy()
        configs["connector"] = self.connector.copy()
        return Channel(uuid=self._uuid, id=self._id, name=self.name, **configs)

    def _copy_configs(self) -> Dict[str, Any]:
        return deepcopy(self.__configs)

    # noinspection PyProtectedMember
    def from_logger(self) -> Channel:
        configs = self.logger._copy_configs()
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
