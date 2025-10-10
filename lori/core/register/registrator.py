# -*- coding: utf-8 -*-
"""
lori.core.register.registrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import inspect
import sys
from collections import OrderedDict
from collections.abc import Callable
from typing import Any, Dict, Optional, TypeVar

from lori._core import _Context, _Entity, _Resource, _Resources  # noqa
from lori._core._registrator import RegistratorContext, _Registrator, _RegistratorContext  # noqa
from lori.core.configs import Configurations, Configurator
from lori.core.errors import ResourceError


class Registrator(_Registrator, Configurator):
    __context: RegistratorContext

    # noinspection PyShadowingBuiltins, PyProtectedMember, PyUnresolvedReferences, PyUnusedLocal
    def __init__(
        self,
        context: RegistratorContext,
        configs: Optional[Configurations] = None,
        id: Optional[str] = None,
        key: Optional[str] = None,
        name: Optional[str] = None,
        **kwargs,
    ) -> None:
        context = self._assert_context(context)

        name = self._build_name(name, configs=configs)
        key = self._build_key(key, configs=configs)
        id = self._build_id(id, key, configs=configs, context=context)

        if configs is None:
            configs = Configurations(f"{key}.conf", context.configs.dirs)
        super().__init__(
            configs=configs,
            id=id,
            key=key,
            name=name,
            **kwargs,
        )
        self.__context = context

    @classmethod
    def _assert_context(
        cls,
        context: RegistratorContext,
    ) -> RegistratorContext:
        if context is None or not isinstance(context, (_RegistratorContext, _Registrator)):
            raise ResourceError(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    # noinspection PyShadowingBuiltins
    def _convert_vars(self, convert: Callable[[Any], str] = str) -> Dict[str, str]:
        vars = self._get_vars()
        values = OrderedDict()
        try:
            id = vars.pop("id", self.id)
            key = vars.pop("key", self.key)
            if id != key:
                values["key"] = id
            values["key"] = key
            values["name"] = vars.pop("name", self.name)
        except (ResourceError, AttributeError):
            # Abstract properties are not yet instanced
            pass

        def is_from_framework(value: Any) -> bool:
            if isinstance(value, (Configurator, _Context, _Entity, _Resource, _Resources)):
                return True
            module = inspect.getmodule(value)
            if module is None:
                return False
            base, *_ = module.__name__.partition(".")
            return sys.modules[base] == "lori"

        values.update({k: str(v) if not is_from_framework(v) else convert(v) for k, v in vars.items()})
        values["enabled"] = str(self.is_enabled())
        values["configured"] = str(self.is_configured())
        values["configs"] = convert(self.configs)
        values["context"] = convert(self.context)
        return values

    @property
    def context(self) -> RegistratorContext:
        return self.__context

    # noinspection PyProtectedMember, PyTypeChecker, PyShadowingBuiltins
    def duplicate(self, context: Optional[RegistratorContext] = None, **changes):
        if context is None:
            context = self.__context
        return super().duplicate(context=context, **changes)


RegistratorType = TypeVar("RegistratorType", bound=Registrator)
