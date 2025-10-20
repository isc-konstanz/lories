# -*- coding: utf-8 -*-
"""
lories._core._configurator
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, TypeVar

from lories._core._configurations import Configurations


class _Configurator(ABC):
    @abstractmethod
    def is_enabled(self) -> bool: ...

    @abstractmethod
    def is_configured(self) -> bool: ...

    @property
    @abstractmethod
    def configs(self) -> Optional[Configurations]: ...

    def configure(self, configs: Configurations) -> None:
        pass

    def update(self, configs: Configurations) -> None:
        pass

    @abstractmethod
    def copy(self) -> Configurator: ...

    @abstractmethod
    def duplicate(self, configs: Optional[Configurations] = None, **changes) -> Configurator: ...


Configurator = TypeVar("Configurator", bound=_Configurator)
