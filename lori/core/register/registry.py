# -*- coding: utf-8 -*-
"""
lori.core.register.registry
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import builtins
from typing import Callable, Collection, Dict, Generic, List, Mapping, Optional, Type, TypeVar

from lori.core import Configurations, Context, ResourceException
from lori.core.register import Registrator

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import get_args

except ImportError:
    from typing_extensions import get_args

R = TypeVar("R", bound=Registrator)


# noinspection PyShadowingBuiltins
class Registration(Generic[R]):
    __class: key
    __factory: Callable[[Context | Registrator, Optional[Configurations]], R]

    _key: str
    alias: List[str]

    # noinspection PyTypeChecker
    def __init__(
        self,
        cls: Type[R],
        type: str,
        *alias: str,
        factory: Callable[[Context | Registrator, Optional[Configurations]], R] = None,
    ):
        if not isinstance(type, str):
            raise ResourceException(f"Invalid '{builtins.type(type)}' registration type: {type}")
        self._key = type.lower()
        self.alias = list(a.lower() for a in alias if a is not None and isinstance(a, str))
        self.__class = cls

        if factory is not None:
            if not callable(factory):
                raise ResourceException(f"Invalid registration initialization function: {factory}")
            self.__factory = factory
        else:
            self.__factory = cls

    @property
    def key(self) -> str:
        return self._key

    @property
    def name(self) -> str:
        return self.__class.__name__

    @property
    def type(self) -> Type[R]:
        return self.__class

    def is_type(self, type: str) -> bool:
        return self.key == type or self.is_alias(type)

    def is_alias(self, type: str) -> bool:
        return any(type.startswith(a) for a in self.alias)

    def is_instance(self, registrator: R) -> bool:
        return isinstance(registrator, self.__class)

    # noinspection PyTypeChecker
    def initialize(self, *args, **kwargs) -> R:
        # registrator = self.__factory(*args, **kwargs)
        # if not isinstance(registrator, self.__class):
        #     raise ResourceException(f"{self.name} factory instanced invalid '{type(registrator)}' registrator")
        return self.__factory(*args, **kwargs)


class RegistrationException(ResourceException):
    """
    Raise if an error with the registration occurred.

    """


# noinspection PyShadowingBuiltins
class Registry(Generic[R]):
    __types: Mapping[Registration[R]]

    def __init__(self) -> None:
        self.__types: Dict[str, Registration[R]] = {}

    # noinspection PyTypeChecker, PyUnresolvedReferences
    def register(
        self,
        cls: Type[R],
        key: str,
        *alias: str,
        factory: Callable = None,
        replace: bool = False,
    ) -> None:
        if not isinstance(key, str):
            raise RegistrationException(f"Invalid '{builtins.type(key)}' registration type: {key}")
        key = key.lower()
        type = get_args(self.__orig_class__)[0]
        if not issubclass(cls, type):
            raise ValueError(f"Can only register {type} types")
        if self.has_type(key) and not replace:
            raise RegistrationException(
                f"Registration '{key}' does already exist: "
                f"{next(t for t in self.__types.values() if key == t.key).name}"
            )
        registration = Registration[R](cls, key, *alias, factory=factory)
        self.__types[key] = registration

    # noinspection PyShadowingBuiltins
    def filter(self, filter: Callable[[Registration[R]], bool]) -> Collection[Registration[R]]:
        return [c for c in self.__types.values() if filter(c)]

    def get_types(self) -> Collection[str]:
        return self.__types.keys()

    def has_type(self, type: str) -> bool:
        return any(t.is_type(type) for t in self.__types.values())

    def from_type(self, type: str) -> Registration[R]:
        for registration in self.__types.values():
            if registration.is_type(type):
                return registration
        raise RegistrationException(f"Registration '{type}' does not exist")
