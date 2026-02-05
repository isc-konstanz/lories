# -*- coding: utf-8 -*-
"""
lories._core._tasks
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Optional, TypeVar

import pandas as pd
from lories._core._activator import _Activator
from lories._core._channels import Channels
from lories._core._connector import Connectors
from lories._core._converter import Converters
from lories._core.typing import Timestamp

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class _TaskContext(_Activator):

    @property
    @abstractmethod
    def converters(self) -> Converters: ...

    @property
    @abstractmethod
    def connectors(self) -> Connectors: ...

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def has_logged(
        self,
        channels: Channels,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> bool: ...

    @abstractmethod
    def read_logged(
        self,
        channels: Channels,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def read(
        self,
        channels: Channels,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def write(
        self,
        data: pd.DataFrame,
        channels: Channels,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> None: ...


TaskContext = TypeVar("TaskContext", bound=_TaskContext)
