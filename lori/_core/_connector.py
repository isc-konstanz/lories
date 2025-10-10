# -*- coding: utf-8 -*-
"""
lori._core._connectors
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from enum import Enum
from typing import Collection, TypeAlias, TypeVar

import pandas as pd
from lori._core._channels import Channels
from lori._core._registrator import _Registrator, _RegistratorContext
from lori._core._resources import Resources


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


ConnectorContext = TypeVar(
    name="ConnectorContext",
    bound=_ConnectorContext[_Connector],
)
Connectors: TypeAlias = ConnectorContext
