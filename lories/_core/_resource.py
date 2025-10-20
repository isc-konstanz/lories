# -*- coding: utf-8 -*-
"""
lories._core._resource
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, Optional, Sequence, Type, TypeVar

from lories._core._entity import _Entity


class _Resource(_Entity):
    @abstractmethod
    def full_name(self, unit: bool = False) -> str: ...

    @property
    @abstractmethod
    def group(self) -> str: ...

    @property
    @abstractmethod
    def unit(self) -> Optional[str]: ...

    @property
    @abstractmethod
    def type(self) -> Type: ...

    @abstractmethod
    def get(self, attr: str, default: Optional[Any] = None) -> Any: ...

    @abstractmethod
    def to_list(self) -> Sequence[Resource]: ...

    @abstractmethod
    def to_configs(self) -> Dict[str, Any]: ...


Resource = TypeVar("Resource", bound=_Resource)
