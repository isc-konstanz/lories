# -*- coding: utf-8 -*-
"""
lories._core._constant
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Type


# noinspection PyPep8Naming, PyShadowingBuiltins
class _Constant(ABC, str):
    @property
    @abstractmethod
    def type(self) -> Type: ...

    @property
    @abstractmethod
    def key(self) -> str: ...

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def unit(self) -> Optional[str]: ...

    @abstractmethod
    def full_name(self, unit: bool = False) -> str: ...
