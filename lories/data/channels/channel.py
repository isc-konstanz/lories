# -*- coding: utf-8 -*-
"""
lories.data.channels.channel
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from typing import Any, Collection, Dict, List, Optional, Type

import pandas as pd
import pytz as tz
from lories._core._channel import ChannelState, _Channel  # noqa
from lories._core._data import DataContext, DataManager, _DataManager  # noqa
from lories._core.typing import Timestamp  # noqa
from lories.core import Resource, ResourceError
from lories.data.channels import ChannelConnector, ChannelConverter, Channels
from lories.util import parse_freq, to_timedelta

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class Channel(_Channel, Resource):
    __context: _DataContext

    _timestamp: pd.Timestamp = pd.NaT
    _value: Optional[Any] = None
    _state: str | ChannelState = ChannelState.DISABLED

    logger: ChannelConnector
    connector: ChannelConnector
    converter: ChannelConverter

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        id: str = None,
        key: str = None,
        name: str = None,
        type: str | Type = None,
        context: DataManager = None,
        converter: ChannelConverter = None,
        connector: Optional[ChannelConnector] = None,
        logger: Optional[ChannelConnector] = None,
        **configs: Any,
    ) -> None:
        super().__init__(id=id, key=key, name=name, type=type, **configs)
        self.__context = self._assert_context(context)
        self.converter = self._assert_converter(converter)
        self.connector = self._assert_connector(connector)
        self.logger = self._assert_connector(logger)

    @classmethod
    def _assert_context(cls, context: DataManager) -> DataManager:
        if context is None or not isinstance(context, _DataManager):
            raise ResourceError(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    @classmethod
    def _assert_converter(cls, converter: Optional[ChannelConverter]) -> ChannelConverter:
        if converter is None or not isinstance(converter, ChannelConverter):
            raise ResourceError(f"Invalid channel converter: {type(converter)}")
        return converter

    @classmethod
    def _assert_connector(cls, connector: Optional[ChannelConnector]) -> ChannelConnector:
        if connector is None:
            connector = ChannelConnector()
        elif not isinstance(connector, ChannelConnector):
            raise ResourceError(f"Invalid channel connector: {type(connector)}")
        return connector

    def _get_attrs(self) -> List[str]:
        return [
            *super()._get_attrs(),
            "converter",
            "connector",
            "logger",
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
        vars = OrderedDict(key=self.id)
        if self.is_valid():
            vars["value"] = str(self.value)
        else:
            vars["state"] = str(self.state)
        vars["timestamp"] = str(self.timestamp)
        return f"{type(self).__name__}({', '.join(f'{k}={v}' for k, v in vars.items())})"

    @property
    def freq(self) -> Optional[str]:
        freq = self.get(next((k for k in ["freq", "frequency", "resolution"] if k in self), None), default=None)
        if freq is not None:
            freq = parse_freq(freq)
        return freq

    @property
    def timedelta(self) -> Optional[pd.Timedelta]:
        return to_timedelta(self.freq)

    @property
    def timestamp(self) -> pd.Timestamp:
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

    def is_valid(self) -> bool:
        return self._is_valid(self.value, self.state)

    @staticmethod
    def _is_valid(value: Any, state: ChannelState) -> bool:
        return state == ChannelState.VALID and not Channel._is_empty(value)

    @staticmethod
    def _is_empty(value: Any) -> bool:
        if isinstance(value, Collection) and not isinstance(value, str) and not isinstance(value, bytes):
            return any(pd.isna(value))
        return pd.isna(value)

    def set(
        self,
        timestamp: pd.Timestamp,
        value: Any,
        state: Optional[str | ChannelState] = ChannelState.VALID,
    ) -> None:
        value = self.converter(value)
        self._set(timestamp, value, state)

    # noinspection PyUnresolvedReferences
    def _set(
        self,
        timestamp: pd.Timestamp,
        value: Optional[Any],
        state: str | ChannelState,
    ) -> None:
        if not isinstance(timestamp, pd.Timestamp):
            raise ResourceError(f"Expected pandas Timestamp for '{self.id}', not: {type(value)}")
        self._timestamp = timestamp

        if self._is_empty(value) and state == ChannelState.VALID:
            raise ResourceError(f"Invalid value for valid state '{self.id}': {value}")
        self._value = value
        self._state = state

    # noinspection PyShadowingBuiltins, PyProtectedMember
    def _update(
        self,
        converter: Optional[Dict[str, Any] | str] = None,
        connector: Optional[Dict[str, Any] | str] = None,
        logger: Optional[Dict[str, Any] | str] = None,
        **configs: Any,
    ) -> None:
        if converter is not None:
            converter = Channel._build_member(converter, "converter")
            self.converter._update(**converter)
        if connector is not None:
            connector = Channel._build_member(connector, "connector")
            self.connector._update(**connector)
        if logger is not None:
            logger = Channel._build_member(logger, "connector")
            self.logger._update(**logger)
        super()._update(**configs)

    def _copy_args(self) -> Dict[str, Any]:
        arguments = super()._copy_args()
        arguments["context"] = self.__context
        # arguments["processors"] = self.processors.copy()
        arguments["converter"] = self.converter.copy()
        arguments["connector"] = self.connector.copy()
        arguments["logger"] = self.logger.copy()

        return arguments

    def copy(self) -> Channel:
        channel = super().copy()
        channel._timestamp = self._timestamp
        channel._value = self._value
        channel._state = self._state
        return channel

    def to_list(self) -> Channels:
        return Channels([self])

    def to_configs(self) -> Dict[str, Any]:
        configs = super().to_configs()
        configs["converter"] = self.converter.to_configs()
        configs["connector"] = self.connector.to_configs()
        configs["logger"] = self.logger.to_configs()
        return configs

    def to_series(self, state: bool = False) -> pd.Series:
        if not self.is_valid() and state:
            return pd.Series(data=[self.state], index=[self.timestamp], name=self.key)

        return self.converter.to_series(self.value, self.timestamp, name=self.key)

    # noinspection PyProtectedMember
    def from_logger(self) -> Channel:
        channel = self.copy()
        channel._update(**self.logger._copy_configs())
        return channel

    # noinspection PyShadowingBuiltins
    def has_logger(self, *ids: Optional[str]) -> bool:
        return self.logger.enabled and (any(self.logger.id == id for id in ids) if len(ids) > 0 else True)

    # noinspection PyShadowingBuiltins
    def has_connector(self, id: Optional[str] = None) -> bool:
        return self.connector.enabled and (self.connector.id == id if id is not None else True)

    # noinspection PyUnresolvedReferences
    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None:
        self.__context.register(function, self, how=how, unique=unique)

    # noinspection PyUnresolvedReferences
    def read(
        self,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame:
        return self.__context.read(self.to_list(), start, end)

    # noinspection PyUnresolvedReferences
    def write(self, data: pd.DataFrame | pd.Series | Any) -> None:
        if data is None:
            raise ResourceError(f"Invalid data to write '{self.id}': {data}")
        if isinstance(data, pd.Series):
            data.name = self.id
            data = data.to_frame()
        elif not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(index=[pd.Timestamp.now(tz.UTC).floor(freq="s")], data=[data], columns=[self.id])

        self.__context.write(data, self.to_list())
