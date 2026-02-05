# -*- coding: utf-8 -*-
"""
lories._core._manager
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Optional, TypeVar, overload

from lories._core._component import Components
from lories._core._listener import Listeners
from lories._core._data import _DataContext
from lories._core._entity import _Entity
from lories._core._tasks import _TaskContext

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class _DataManager(_DataContext, _TaskContext, _Entity):

    @property
    @abstractmethod
    def components(self) -> Components: ...

    @property
    @abstractmethod
    def listeners(self) -> Listeners: ...


DataManager = TypeVar("DataManager", bound=_DataManager)
