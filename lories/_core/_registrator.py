# -*- coding: utf-8 -*-
"""
lories._core._registrator
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import re
from abc import abstractmethod
from copy import deepcopy
from typing import Any, Collection, Dict, Generic, Optional, Type, TypeVar, Union, overload

from lories._core._configurations import Configurations
from lories._core._configurator import _Configurator
from lories._core._context import _Context
from lories._core._entity import _Entity
from lories.util import validate_key


class _Registrator(_Configurator, _Entity):
    INCLUDES: Collection[str] = ()
    TYPE: str = "registrator"

    @property
    @abstractmethod
    def context(self) -> RegistratorContext: ...

    # noinspection PyShadowingBuiltins
    @classmethod
    def _build_id(
        cls,
        id: Optional[str] = None,
        key: Optional[str] = None,
        context: Optional[RegistratorContext] = None,
        configs: Optional[Configurations] = None,
    ) -> str:
        if configs is not None:
            if id is None:
                if configs.has_member(cls.TYPE) and "id" in configs[cls.TYPE]:
                    id = configs[cls.TYPE]["id"]
                elif "id" in configs:
                    id = configs["id"]
            if key is None:
                if configs.has_member(cls.TYPE) and "key" in configs[cls.TYPE]:
                    key = configs[cls.TYPE]["key"]
                elif "key" in configs:
                    key = configs["key"]
                else:
                    key = validate_key("_".join(re.split(r"[^\w-]", configs.key)))
        if key is None:
            raise ValueError(f"Unable to build '{cls.__name__}' for missing ID")
        if id is None and context is not None and isinstance(context, _Registrator):
            id = f"{context.id}.{key}"
        else:
            id = key
        return id

    @classmethod
    def _build_key(cls, key: Optional[str], configs: Optional[Configurations]) -> str:
        if configs is not None:
            if configs.has_member(cls.TYPE) and "key" in configs[cls.TYPE]:
                key = configs[cls.TYPE]["key"]
            elif "key" in configs:
                key = configs["key"]
            else:
                if configs.has_member(cls.TYPE) and "name" in configs[cls.TYPE]:
                    key = validate_key(configs[cls.TYPE]["name"])
                elif "name" in configs:
                    key = validate_key(configs["name"])
                elif key is None:
                    key = validate_key(configs.key)
        if key is None:
            raise ValueError(f"Unable to build '{cls.__name__}' for missing key")
        return key

    @classmethod
    def _build_name(cls, name: Optional[str], configs: Optional[Configurations]) -> str:
        if configs is not None:
            if configs.has_member(cls.TYPE) and "name" in configs[cls.TYPE]:
                name = configs[cls.TYPE]["name"]
            elif "name" in configs:
                name = configs["name"]
        return name

    @classmethod
    def _build_defaults(
        cls,
        configs: Configurations,
        includes: Optional[Collection[str]] = (),
        **kwargs,
    ) -> Dict[str, Any]:
        defaults = {k: deepcopy(v) for k, v in configs.items() if k in cls.INCLUDES or k in includes}
        defaults.update(**kwargs)
        return defaults


Registrator = TypeVar("Registrator", bound=_Registrator)


# noinspection PyAbstractClass, PyShadowingBuiltins
class _RegistratorContext(_Context[Registrator], Generic[Registrator]):
    TYPE: str = "registrators"

    @overload
    def get_all(self) -> Collection[Registrator]: ...

    @overload
    def get_all(self, type: str, *other: str) -> Collection[Registrator]: ...

    @overload
    def get_all(self, type: Type[Registrator], *other: Type[Registrator]) -> Collection[Registrator]: ...

    def get_all(self, *types: str | Type[Registrator]) -> Collection[Registrator]: ...

    @overload
    def get_first(self) -> Collection[Registrator]: ...

    @overload
    def get_first(self, type: str) -> Collection[Registrator]: ...

    @overload
    def get_first(self, type: str, *other: str) -> Collection[Registrator]: ...

    @overload
    def get_first(self, type: Type[Registrator], *other: Type[Registrator]) -> Collection[Registrator]: ...

    def get_first(self, *types: str | Type[Registrator]) -> Optional[Registrator]: ...

    @overload
    def get_last(self) -> Collection[Registrator]: ...

    @overload
    def get_last(self, type: str, *other: str) -> Collection[Registrator]: ...

    @overload
    def get_last(self, type: Type[Registrator], *other: Type[Registrator]) -> Collection[Registrator]: ...

    def get_last(self, *types: str | Type[Registrator]) -> Optional[Registrator]: ...

    @overload
    def has_type(self, type: str, *other: str) -> bool: ...

    @overload
    def has_type(self, type: Type[Registrator], *other: Type[Registrator]) -> bool: ...

    def has_type(self, *types: str | Type[Registrator]) -> bool: ...


RegistratorContext = TypeVar(
    name="RegistratorContext",
    bound=Union[_RegistratorContext[_Registrator], _Registrator],
)
