# -*- coding: utf-8 -*-
"""
lori.core.register.registry
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import builtins
from typing import Callable, Collection, Dict, Generic, Mapping, Optional, Type

from lori._core._registrator import Registrator  # noqa
from lori.core.register.registration import Registration, RegistrationError, RegistrationFactory

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import get_args

except ImportError:
    from typing_extensions import get_args


# noinspection PyShadowingBuiltins
class Registry(Generic[Registrator]):
    __types: Mapping[str, Registration[Registrator]]

    def __init__(self) -> None:
        self.__types: Dict[str, Registration[Registrator]] = {}

    def register(
        self,
        cls: Type[Registrator],
        key: str,
        *alias: str,
        factory: Optional[RegistrationFactory] = None,
        replace: bool = False,
    ) -> None:
        if not isinstance(key, str):
            raise RegistrationError(f"Invalid '{builtins.type(key)}' registration type: {key}")
        key = key.lower()
        type = self._get_generic_type()
        if not issubclass(cls, type):
            raise ValueError(f"Can only register {type} types")
        if self.has_type(key) and not replace:
            raise RegistrationError(
                f"Registration '{key}' does already exist: "
                f"{next(t for t in self.__types.values() if key == t.key).name}"
            )
        registration = Registration[Registrator](cls, key, *alias, factory=factory)
        self.__types[key] = registration

    # noinspection PyShadowingBuiltins
    def filter(
        self,
        filter: Callable[[Registration[Registrator]], bool],
    ) -> Collection[Registration[Registrator]]:
        return [c for c in self.__types.values() if filter(c)]

    def get_types(self) -> Collection[str]:
        return self.__types.keys()

    def has_type(self, type: str) -> bool:
        return any(t.is_type(type) for t in self.__types.values())

    def from_type(self, type: str) -> Registration[Registrator]:
        for registration in self.__types.values():
            if registration.is_type(type):
                return registration
        raise RegistrationError(f"Registration '{type}' does not exist")

    # noinspection PyTypeChecker, PyUnresolvedReferences
    def _get_generic_type(self) -> Type[Registrator]:
        return get_args(self.__orig_class__)[0]
