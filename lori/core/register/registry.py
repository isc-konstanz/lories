# -*- coding: utf-8 -*-
"""
lori.core.register.registry
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import builtins
from typing import Callable, Generic, List, Mapping, Optional, Type, TypeVar, get_args

from lori.core import Configurations, Context, ResourceException
from lori.core.register import Registrator

R = TypeVar("R", bound=Registrator)


# noinspection PyShadowingBuiltins
class Registration(Generic[R]):
    __class: type
    __context: Context
    __factory: Callable[[Registrator | Context, Optional[Configurations]], R]

    _type: str
    alias: List[str]

    # noinspection PyTypeChecker
    def __init__(
        self,
        cls: Type[R],
        type: str,
        *alias: str,
        factory: Callable[[Registrator | Context, Optional[Configurations]], R] = None,
    ):
        if not isinstance(type, str):
            raise ResourceException(f"Invalid '{builtins.type(type)}' registration type: {type}")
        self._type = type.lower()
        self.alias = list(a.lower() for a in alias if a is not None and isinstance(a, str))
        self.__class = cls

        if factory is not None:
            if not callable(factory):
                raise ResourceException(f"Invalid registration initialization function: {factory}")
            self.__factory = factory
        else:
            self.__factory = cls

    @property
    def type(self):
        return self._type

    @property
    def name(self):
        return self.__class.__name__

    def is_type(self, type: str) -> bool:
        return self.type == type or self.is_alias(type)

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


# noinspection PyShadowingBuiltins
class Registry(Generic[R]):
    types: Mapping[Registration[R]]

    def __init__(self) -> None:
        self.types: dict[str, Registration[R]] = {}

    # noinspection PyTypeChecker, PyUnresolvedReferences
    def register(
        self,
        cls: Type[R],
        type: str,
        *alias: str,
        factory: Callable = None,
        replace: bool = False,
    ) -> None:
        if not isinstance(type, str):
            raise ResourceException(f"Invalid '{builtins.type(type)}' registration type: {type}")
        type = type.lower()

        registration_type = get_args(self.__orig_class__)[0]
        if not issubclass(cls, registration_type):
            raise ValueError(f"Can only register {registration_type} types")
        if self.has_type(type) and not replace:
            raise ResourceException(
                f"Registration '{type}' does already exist: "
                f"{next(t for t in self.types.values() if type == t.type).name}"
            )
        registration = Registration(cls, type, *alias, factory=factory)
        self.types[type] = registration

    def has_type(self, type: str) -> bool:
        return any(t.is_type(type) for t in self.types.values())
