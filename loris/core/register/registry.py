# -*- coding: utf-8 -*-
"""
loris.core.register.registry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Callable, Generic, List, Mapping, Type, TypeVar, get_args

from loris.core import ResourceException
from loris.core.register import Registrator

R = TypeVar("R", bound=Registrator)


# noinspection PyShadowingBuiltins
class Registration(Generic[R]):
    _class: type
    _factory: callable

    type: str
    alias: List[str]

    def __init__(self, cls: Type[R], *alias: str, factory: Callable = None):
        self.type = cls.TYPE
        self._class = cls
        self._factory = cls if factory is None else factory
        self.alias = [a for a in alias if a is not None and a != self.type]

    @property
    def name(self):
        return self._class.__name__

    def is_type(self, type: str) -> bool:
        return self.type == type or self.is_alias(type)

    def is_alias(self, type: str) -> bool:
        return any(type.startswith(a) for a in self.alias)

    def initialize(self, *args, **kwargs) -> R:
        factory = self._factory
        if factory is None:
            factory = self._class
        elif not callable(factory):
            raise ResourceException(f"Invalid registration initialization function: {factory}")

        return factory(*args, **kwargs)


# noinspection PyShadowingBuiltins
class Registry(Generic[R]):
    types: Mapping[Registration[R]]

    def __init__(self) -> None:
        self.types: dict[str, Registration[R]] = {}

    # noinspection PyTypeChecker, PyUnresolvedReferences
    def register(self, cls: Type[R], *alias: str, factory: Callable = None, replace: bool = False) -> None:
        type = get_args(self.__orig_class__)[0]
        if not issubclass(cls, type):
            raise ValueError("Can only register Registrator types")
        if self.has_type(cls.TYPE) and not replace:
            raise ResourceException(
                f"Registration '{cls.TYPE}' does already exist: "
                f"{next(t for t in self.types.values() if cls.TYPE == t.type).name}"
            )
        self.types[cls.TYPE] = Registration(cls, *alias, factory=factory)

    def has_type(self, type: str) -> bool:
        return any(t.is_type(type) for t in self.types.values())
