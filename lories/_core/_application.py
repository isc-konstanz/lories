# -*- coding: utf-8 -*-
"""
lories._core._application
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod

from lories._core._component import Components
from lories._core._data import _DataContext
from lories._core._entity import _Entity
from lories._core._listener import Listeners
from lories._core._tasks import _TaskContext


class _Application(_DataContext, _TaskContext, _Entity):
    @property
    @abstractmethod
    def components(self) -> Components: ...

    @property
    @abstractmethod
    def listeners(self) -> Listeners: ...
