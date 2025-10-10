# -*- coding: utf-8 -*-
"""
lori._core._configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from typing import Any, Collection, Iterable, Mapping, Optional, Sequence, TypeVar

import pandas as pd


class _Configurations(ABC, MutableMapping[str, Any]):
    @property
    @abstractmethod
    def key(self) -> str: ...

    @abstractmethod
    def remove(self, *keys: str) -> None: ...

    @abstractmethod
    def pop(self, key: str, default: Any = None) -> Any: ...

    @abstractmethod
    def set(self, key: str, value: Any, replace: bool = True) -> None: ...

    @abstractmethod
    def get(self, key: str | Iterable[str], default: Any = None) -> Any: ...

    @abstractmethod
    def get_bool(self, key: str, default: bool = None) -> bool: ...

    @abstractmethod
    def get_int(self, key: str, default: int = None) -> int: ...

    @abstractmethod
    def get_float(self, key: str, default: float = None) -> float: ...

    def get_date(self, key: str, default: pd.Timestamp | dt.datetime = None, **kwargs) -> pd.Timestamp: ...

    @abstractmethod
    def move_to_top(self, key: str) -> None: ...

    @abstractmethod
    def move_to_bottom(self, key: str) -> None: ...

    @abstractmethod
    def write(self, **kwargs) -> None: ...

    @abstractmethod
    def copy(self, **kwargs) -> Configurations: ...

    @property
    @abstractmethod
    def enabled(self) -> bool: ...

    @property
    @abstractmethod
    def sections(self) -> Sequence[str]: ...

    @abstractmethod
    def has_section(self, section: str, includes: bool = False) -> bool: ...

    @abstractmethod
    def get_sections(
        self,
        sections: Collection[str],
        ensure_exists: bool = False,
    ) -> Configurations: ...

    @abstractmethod
    def get_section(
        self,
        section: str,
        defaults: Optional[Mapping[str, Any]] = None,
        ensure_exists: bool = False,
    ) -> Configurations: ...

    @abstractmethod
    def pop_section(
        self,
        section: str,
        defaults: Optional[Mapping[str, Any]] = None,
    ) -> Configurations: ...

    @abstractmethod
    def update(self, update: Mapping[str, Any], replace: bool = True) -> None: ...


Configurations = TypeVar("Configurations", bound=_Configurations)
