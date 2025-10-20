# -*- coding: utf-8 -*-
"""
lories.data.channels.channel
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from copy import deepcopy
from enum import Enum
from typing import Any, Collection, Dict, Mapping, Optional, TypeVar, overload

import pandas as pd
from lories._core._configurations import Configurations
from lories._core._context import Context
from lories._core._entity import Entity, _Entity
from lories._core._resource import _Resource
from lories._core.typing import Timestamp

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class ChannelState(Enum):
    DISABLED = "DISABLED"

    DISCONNECTING = "DISCONNECTING"
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"

    VALID = "VALID"

    NOT_AVAILABLE = "NOT_AVAILABLE"

    TIMEOUT = "TIMEOUT"
    READ_ERROR = "READ_ERROR"
    WRITE_ERROR = "WRITE_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"
    ARGUMENT_SYNTAX_ERROR = "ARGUMENT_SYNTAX_ERROR"

    def __str__(self):
        return str(self.value)


class _Channel(_Resource):
    INCLUDES: Collection[str] = (
        "logger",
        "connector",
        "converter",
        "replicator",
        "replication",
        "retention",
        "rotate",
    )
    TYPE: str = "channel"
    TIMESTAMP: str = "timestamp"

    @property
    @abstractmethod
    def freq(self) -> Optional[str]: ...

    @property
    @abstractmethod
    def timedelta(self) -> Optional[pd.Timedelta]: ...

    @property
    @abstractmethod
    def timestamp(self) -> pd.Timestamp | pd.NaT: ...

    @property
    @abstractmethod
    def value(self) -> Optional[Any]: ...

    @value.setter
    @abstractmethod
    def value(self, value) -> None: ...

    @property
    @abstractmethod
    def state(self) -> ChannelState | str: ...

    @state.setter
    @abstractmethod
    def state(self, state) -> None: ...

    @abstractmethod
    def is_valid(self) -> bool: ...

    @overload
    def set(
        self,
        timestamp: pd.Timestamp,
        value: Any,
    ) -> None: ...

    @abstractmethod
    def set(
        self,
        timestamp: pd.Timestamp,
        value: Any,
        state: Optional[str | ChannelState] = ChannelState.VALID,
    ) -> None: ...

    @abstractmethod
    def to_series(self, state: bool = False) -> pd.Series: ...

    @abstractmethod
    def from_logger(self) -> Channel: ...

    @abstractmethod
    def has_logger(self, *ids: Optional[str]) -> bool: ...

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def has_connector(self, id: Optional[str] = None) -> bool: ...

    @abstractmethod
    def register(
        self,
        function: Callable[[pd.DataFrame], None],
        how: Literal["any", "all"] = "any",
        unique: bool = False,
    ) -> None: ...

    @abstractmethod
    def read(
        self,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame: ...

    @overload
    def write(self, data: Any) -> None: ...

    @overload
    def write(self, data: pd.Series) -> None: ...

    @overload
    def write(self, data: pd.DataFrame) -> None: ...

    @abstractmethod
    def write(self, data: pd.DataFrame | pd.Series | Any) -> None: ...

    # noinspection PyShadowingBuiltins
    @classmethod
    def _build_id(
        cls,
        id: Optional[str] = None,
        key: Optional[str] = None,
        context: Optional[Context | Entity] = None,
    ) -> str:
        if id is None:
            if key is None:
                raise ValueError(f"Unable to build '{cls.__name__}' for missing ID")
            if id is None and context is not None and isinstance(context, _Entity):
                id = f"{context.id}.{key}"
            else:
                id = key
        return id

    @staticmethod
    def _build_defaults(configs: Configurations) -> Dict[str, Any]:
        return _Channel._build_configs(
            {k: deepcopy(v) for k, v in configs.items() if not isinstance(v, Mapping) or k in _Channel.INCLUDES}
        )

    @staticmethod
    def _build_configs(configs: Dict[str, Any]) -> Dict[str, Any]:
        def _build_wrapper(_key: str, _type: Optional[str] = None) -> None:
            if _type is None:
                _type = _key
            if _type not in configs:
                return
            configs[_type] = _Channel.__build_member(configs[_type], _key)

        _build_wrapper("converter")
        _build_wrapper("connector")
        _build_wrapper("connector", "logger")
        return configs

    @staticmethod
    def __build_member(configs: Optional[Dict[str, Any] | str], key: str) -> Optional[Dict[str, Any]]:
        if isinstance(configs, str) or configs is None:
            return {key: configs}
        elif not isinstance(configs, Mapping):
            raise ValueError(f"Invalid channel {key} type: " + str(configs))
        return dict(configs)


Channel = TypeVar("Channel", bound=_Channel)
