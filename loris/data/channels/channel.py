# -*- coding: utf-8 -*-
"""
loris.data.channels.channel
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from typing import Any, Dict, List, Optional, Type

import pandas as pd
import pytz as tz
from loris.core import Resource
from loris.data.channels import ChannelConnector, ChannelState
from loris.util import _parse_freq, to_timedelta


class Channel(Resource):
    logger: ChannelConnector
    connector: ChannelConnector

    _timestamp: pd.Timestamp = pd.NaT
    _value: Optional[Any] = None
    _state: str | ChannelState = ChannelState.DISABLED

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        id: str = None,
        key: str = None,
        name: Optional[str] = None,
        **configs: Any,
    ) -> None:
        connector = self.__parse_connector(configs.pop("connector", {}))
        logger = self.__parse_connector(configs.pop("logger", {}))

        super().__init__(id, key, name, **configs)
        self.connector = connector
        self.logger = logger

    @staticmethod
    def __parse_connector(connector: ChannelConnector | Dict[str, Any]) -> ChannelConnector:
        if isinstance(connector, ChannelConnector):
            return connector

        if connector is None or isinstance(connector, str):
            connector = {"connector": connector}
        return ChannelConnector(**connector)

    def _get_attrs(self) -> List[str]:
        return [
            *super()._get_attrs(),
            "logger",
            "connector",
            "value",
            "state",
            "timestamp",
        ]

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()
        vars["value"] = self.value
        vars["state"] = self.state
        vars["timestamp"] = self.timestamp
        return vars

    # noinspection PyShadowingBuiltins
    def __repr__(self) -> str:
        vars = OrderedDict(key=self.key)
        if self.is_valid():
            vars["value"] = str(self.value)
        else:
            vars["state"] = str(self.state)
        vars["timestamp"] = str(self.timestamp)
        return f"{type(self).__name__}({', '.join(f'{k}={v}' for k, v in vars.items())})"

    @property
    def type(self) -> Optional[Type]:
        if (self._type is None
                and self._value is not None):
            return type(self._value)
        return self._type

    @property
    def freq(self) -> Optional[str]:
        freq = self.get(next((k for k in ["freq", "frequency", "resolution"] if k in self), None), default=None)
        if freq is not None:
            freq = _parse_freq(freq)
        return freq

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
        configs = self._get_vars()
        configs["logger"] = self.logger.copy()
        configs["connector"] = self.connector.copy()
        return Channel(**configs)

    # noinspection PyProtectedMember
    def from_logger(self) -> Channel:
        configs = {k: v for k, v in self.logger._get_vars().items() if k not in ["id", "timestamp"]}
        configs["logger"] = deepcopy(self.logger)
        configs["connector"] = deepcopy(self.connector)
        channel = Channel(id=self._id, key=self._key, name=self.name, **configs)
        channel.set(self._timestamp, self._value, self._state)
        return channel

    # noinspection PyShadowingBuiltins
    def has_logger(self, *ids: Optional[str]) -> bool:
        return any(self.logger.id == id for id in ids) if len(ids) > 0 else self.logger.id is not None

    # noinspection PyShadowingBuiltins
    def has_connector(self, id: Optional[str] = None) -> bool:
        return self.connector.id == id if id is not None else self.connector.id is not None

    def to_series(self, state: bool = False) -> pd.Series:
        if isinstance(self.value, pd.Series):
            return self.value
        else:
            if state and pd.isna(self.value):
                data = self.state
            else:
                data = self.value
            return pd.Series(index=[self.timestamp], data=[data], name=self.key)
