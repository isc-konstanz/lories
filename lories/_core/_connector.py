# -*- coding: utf-8 -*-
"""
lories._core._connectors
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from enum import Enum
from typing import Collection, Optional, TypeAlias, TypeVar

import pandas as pd
from lories._core._channels import Channels, ChannelsArgument
from lories._core._registrator import _Registrator, _RegistratorContext
from lories._core._resources import Resources


class ConnectType(Enum):
    NONE = "NONE"
    AUTO = "AUTO"

    @classmethod
    def get(cls, value: str | bool) -> ConnectType:
        if isinstance(value, str):
            value = value.lower()
            if value in ["auto", "true"]:
                return ConnectType.AUTO
            if value in ["none", "false"]:
                return ConnectType.NONE
        if isinstance(value, bool):
            if value:
                return ConnectType.AUTO
            else:
                return ConnectType.NONE
        raise ValueError("Unknown ConnectType: " + str(value))

    def __str__(self):
        return str(self.value)


class _Connector(_Registrator):
    INCLUDES: Collection[str] = ()
    TYPE: str = "connector"

    @property
    @abstractmethod
    def resources(self) -> Resources: ...

    @property
    @abstractmethod
    def channels(self) -> Channels: ...

    @abstractmethod
    def is_connected(self) -> bool: ...

    def connect(self, resources: Resources) -> None:
        pass

    def disconnect(self) -> None:
        pass

    @abstractmethod
    def read(self, resources: Resources) -> pd.DataFrame: ...

    @abstractmethod
    def write(self, data: pd.DataFrame) -> None: ...


Connector = TypeVar("Connector", bound=_Connector)


# noinspection PyAbstractClass
class _ConnectorContext(_RegistratorContext[Connector]):
    TYPE: str = "connectors"

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def connect(
        self,
        filter: Optional[Callable[[Connector], bool]] = None,
        channels: Optional[ChannelsArgument] = None,
        timeout: Optional[float] = None,
    ) -> None: ...

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def reconnect(
        self,
        filter: Optional[Callable[[Connector], bool]] = None,
    ) -> None: ...

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def disconnect(
        self,
        filter: Optional[Callable[[Connector], bool]] = None,
    ) -> None: ...


ConnectorContext = TypeVar(
    name="ConnectorContext",
    bound=_ConnectorContext,
)
Connectors: TypeAlias = _ConnectorContext
