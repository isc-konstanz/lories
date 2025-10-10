# -*- coding: utf-8 -*-
"""
lori.core.register.registry
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import builtins
from typing import Callable, Generic, List, Optional, Type

from lori._core._configurations import Configurations  # noqa
from lori._core._registrator import Registrator, RegistratorContext  # noqa
from lori.core.errors import ResourceError


# noinspection PyShadowingBuiltins
class Registration(Generic[Registrator]):
    __class: Type[Registrator]
    __factory: RegistrationFactory

    _key: str
    alias: List[str]

    def __init__(
        self,
        cls: Type[Registrator],
        type: str,
        *alias: str,
        factory: Optional[RegistrationFactory] = None,
    ):
        if not isinstance(type, str):
            raise ResourceError(f"Invalid '{builtins.type(type)}' registration type: {type}")
        self._key = type.lower()
        self.alias = list(a.lower() for a in alias if a is not None and isinstance(a, str))
        self.__class = cls

        if factory is not None:
            if not callable(factory):
                raise ResourceError(f"Invalid registration initialization function: {factory}")
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
    def type(self) -> Type[Registrator]:
        return self.__class

    def is_type(self, type: str) -> bool:
        return self.key == type or self.is_alias(type)

    def is_alias(self, type: str) -> bool:
        return any(type.startswith(a) for a in self.alias)

    def is_instance(self, registrator: Registrator) -> bool:
        return isinstance(registrator, self.__class)

    def initialize(self, *args, **kwargs) -> Registrator:
        # registrator = self.__factory(*args, **kwargs)
        # if not isinstance(registrator, self.__class):
        #     raise ResourceError(f"{self.name} factory instanced invalid '{type(registrator)}' registrator")
        return self.__factory(*args, **kwargs)


class RegistrationError(ResourceError):
    """
    Raise if an error with the registration occurred.

    """


RegistrationFactory = Callable[[RegistratorContext, Optional[Configurations]], Registrator]
