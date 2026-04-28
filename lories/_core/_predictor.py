# -*- coding: utf-8 -*-
"""
lories.data.predictors.core
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import Any, Collection, Dict, Optional, TypeAlias, TypeVar, overload

import pandas as pd
from lories._core._activator import _Activator
from lories._core._channel import _Channel
from lories._core._channels import ChannelsArgument, _Channels
from lories._core._configurations import Configurations
from lories._core._data import Data, _DataContext
from lories._core._registrator import _Registrator, _RegistratorContext
from lories._core._resources import Resources
from lories._core.typing import Timestamp


# noinspection PyShadowingBuiltins
class _Predictor(_Registrator, _Activator):
    INCLUDES: Collection[str] = (_DataContext.TYPE,)
    TYPE: str = "predictor"

    def activate(self, resources: Resources) -> None:
        pass

    @property
    @abstractmethod
    def data(self) -> Data: ...

    @overload
    def predict(self, resources: Resources) -> pd.DataFrame: ...

    @overload
    def predict(self, resources: Resources, date: Timestamp | str) -> pd.DataFrame: ...

    @overload
    def predict(
        self,
        resources: Resources,
        start: Timestamp | str,
        end: Timestamp | str,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def predict(
        self,
        resources: Resources,
        start: Optional[Timestamp | str] = None,
        end: Optional[Timestamp | str] = None,
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
        exclude = ("freq", "frequency", "resolution", "listener", "listening", "listen")
        defaults = super()._build_defaults(configs, includes)
        if strict and _DataContext.TYPE in defaults:
            defaults[_DataContext.TYPE][_Channels.TYPE] = _Channel._build_defaults(
                defaults[_DataContext.TYPE].get_member(_Channels.TYPE, defaults={}, exclude=exclude)
            )
        return defaults


Predictor = TypeVar("Predictor", bound=_Predictor)


# noinspection PyAbstractClass
class _PredictorContext(_RegistratorContext[Predictor]):
    TYPE: str = "predictors"

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def activate(
        self,
        filter: Optional[Callable[[Predictor], bool]] = None,
        channels: Optional[ChannelsArgument] = None,
    ) -> None: ...

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def deactivate(
        self,
        filter: Optional[Callable[[Predictor], bool]] = None,
    ) -> None: ...


PredictorContext = TypeVar(
    name="PredictorContext",
    bound=_PredictorContext,
)
Predictors: TypeAlias = _PredictorContext
