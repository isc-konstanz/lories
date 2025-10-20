# -*- coding: utf-8 -*-
"""
lories._core._listener
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Optional, TypeVar

import pandas as pd
from lories._core._entity import _Entity


class _Listener(_Entity):
    @property
    @abstractmethod
    def timestamp(self) -> pd.Timestamp | pd.NaT: ...

    @property
    @abstractmethod
    def runtime(self) -> Optional[float]: ...

    @abstractmethod
    def run(self) -> None: ...

    @abstractmethod
    def locked(self) -> bool: ...

    @abstractmethod
    def has_update(self) -> bool: ...


Listener = TypeVar("Listener", bound=_Listener)
