# -*- coding: utf-8 -*-
"""
lori._core._activator
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import TypeVar

from lori._core._configurator import _Configurator


class _Activator(_Configurator):
    @abstractmethod
    def is_active(self) -> bool: ...

    def activate(self) -> None:
        pass

    def deactivate(self) -> None:
        pass


Activator = TypeVar("Activator", bound=_Activator)
