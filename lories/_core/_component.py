# -*- coding: utf-8 -*-
"""
lories._core._component
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import Any, Collection, Dict, Optional, TypeAlias, TypeVar, overload

import pandas as pd
from lories._core._activator import _Activator
from lories._core._channel import _Channel
from lories._core._channels import _Channels
from lories._core._configurations import Configurations
from lories._core._connector import Connectors, _Connector
from lories._core._converter import Converters
from lories._core._data import Data, _DataContext
from lories._core._predictor import Predictors
from lories._core._registrator import _Registrator, _RegistratorContext
from lories._core.typing import Timestamp


class _Component(_Registrator, _Activator):
    INCLUDES: Collection[str] = (_DataContext.TYPE,)
    TYPE: str = "component"

    @property
    @abstractmethod
    def components(self) -> Components: ...

    @property
    @abstractmethod
    def converters(self) -> Converters: ...

    @property
    @abstractmethod
    def connectors(self) -> Connectors: ...

    @property
    @abstractmethod
    def predictors(self) -> Predictors: ...

    @property
    @abstractmethod
    def data(self) -> Data: ...

    @overload
    def get(self, unique: bool = False) -> pd.DataFrame: ...

    @overload
    def get(self, date: Timestamp | str, unique: bool = False) -> pd.DataFrame: ...

    @overload
    def get(
        self,
        start: Timestamp | str,
        end: Timestamp | str,
        unique: bool = False,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def get(
        self,
        start: Optional[Timestamp | str] = None,
        end: Optional[Timestamp | str] = None,
        unique: bool = False,
        **kwargs,
    ) -> pd.DataFrame: ...

    @overload
    def predict(self, unique: bool = False) -> pd.DataFrame: ...

    @overload
    def predict(self, date: Timestamp | str, unique: bool = False) -> pd.DataFrame: ...

    @overload
    def predict(
        self,
        start: Timestamp | str,
        end: Timestamp | str,
        unique: bool = False,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def predict(
        self,
        start: Optional[Timestamp | str] = None,
        end: Optional[Timestamp | str] = None,
        unique: bool = False,
        **kwargs,
    ) -> pd.DataFrame: ...

    # noinspection PyProtectedMember
    @classmethod
    def _build_defaults(
        cls,
        configs: Configurations,
        includes: Optional[Collection[str]] = (),
        strict: bool = False,
    ) -> Dict[str, Any]:
        excludes = (_Connector.TYPE,)
        defaults = super()._build_defaults(configs, includes)
        if strict and _DataContext.TYPE in defaults:
            defaults[_DataContext.TYPE][_Channels.TYPE] = _Channel._build_defaults(
                defaults[_DataContext.TYPE].get_member(_Channels.TYPE, defaults={}, exclude=excludes)
            )
        return defaults


Component = TypeVar("Component", bound=_Component)


# noinspection PyAbstractClass
class _ComponentContext(_RegistratorContext[Component]):
    TYPE: str = "components"

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def activate(self, filter: Optional[Callable[[Component], bool]] = None) -> None: ...

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def deactivate(self, filter: Optional[Callable[[Component], bool]] = None) -> None: ...


ComponentContext = TypeVar(
    name="ComponentContext",
    bound=_ComponentContext,
)
Components: TypeAlias = _ComponentContext
