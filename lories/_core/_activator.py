# -*- coding: utf-8 -*-
"""
lories._core._activator
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import TypeVar, overload

from lories._core._configurator import _Configurator


class _Activator(_Configurator):
    @abstractmethod
    def is_active(self) -> bool: ...

    @overload
    def activate(self) -> None: ...

    @overload
    def activate(self, *args) -> None: ...

    def activate(self, *args) -> None:
        pass

    def deactivate(self) -> None:
        pass


Activator = TypeVar("Activator", bound=_Activator)
