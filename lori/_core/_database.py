# -*- coding: utf-8 -*-
"""
lori._core._database
~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Optional, TypeVar, overload

import pandas as pd
from lori._core._connector import _Connector
from lori._core._resources import Resources
from lori._core.typing import Timestamp

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class _Database(_Connector):
    # noinspection PyShadowingBuiltins
    @abstractmethod
    def hash(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        method: Literal["MD5", "SHA1", "SHA256", "SHA512"] = "MD5",
        encoding: str = "UTF-8",
    ) -> Optional[str]: ...

    def exists(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> bool: ...

    @overload
    def read(self, resources: Resources) -> pd.DataFrame: ...

    @overload
    def read(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def read(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def read_first(self, resources: Resources) -> Optional[pd.DataFrame]: ...

    @abstractmethod
    def read_first_index(self, resources: Resources) -> Optional[Any]: ...

    @abstractmethod
    def read_last(self, resources: Resources) -> Optional[pd.DataFrame]: ...

    @abstractmethod
    def read_last_index(self, resources: Resources) -> Optional[pd.Index]: ...

    @overload
    def delete(self, resources: Resources) -> None: ...

    @overload
    def delete(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> None: ...

    @abstractmethod
    def delete(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> None: ...


Database = TypeVar("Database", bound=_Database)
