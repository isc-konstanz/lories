# -*- coding: utf-8 -*-
"""
lori._core._component
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Collection, Dict, Optional, TypeAlias, TypeVar, overload

import pandas as pd
from lori._core._activator import _Activator
from lori._core._channel import _Channel
from lori._core._channels import _Channels
from lori._core._configurations import Configurations
from lori._core._connector import Connectors
from lori._core._converter import Converters
from lori._core._data import Data, _DataContext
from lori._core._registrator import _Registrator, _RegistratorContext
from lori._core.typing import Timestamp


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
    def data(self) -> Data: ...

    @overload
    def get(self) -> pd.DataFrame: ...

    @overload
    def get(self, date: Timestamp | str) -> pd.DataFrame: ...

    @overload
    def get(
        self,
        start: Timestamp | str,
        end: Timestamp | str,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def get(
        self,
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
        defaults = super()._build_defaults(configs, includes)
        if strict and _DataContext.TYPE in defaults:
            defaults[_DataContext.TYPE][_Channels.TYPE] = _Channel._build_defaults(
                defaults[_DataContext.TYPE].get_member(_Channels.TYPE, defaults={})
            )
        return defaults


Component = TypeVar("Component", bound=_Component)


# noinspection PyAbstractClass
class _ComponentContext(_RegistratorContext[Component]):
    TYPE: str = "components"


ComponentContext = TypeVar(
    name="ComponentContext",
    bound=_ComponentContext[_Component],
)
Components: TypeAlias = ComponentContext
