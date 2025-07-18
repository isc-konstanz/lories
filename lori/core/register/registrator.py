# -*- coding: utf-8 -*-
"""
lori.core.register.registrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import inspect
import os
import sys
from collections import OrderedDict
from collections.abc import Callable
from copy import deepcopy
from typing import Any, Collection, Dict, List, Optional

from lori.core import Configurations, Configurator, Context, Entity, ResourceException
from lori.util import validate_key


class Registrator(Configurator, Entity):
    SECTION: str = "registration"
    INCLUDES: List[str] = []

    __context: Context

    # noinspection PyShadowingBuiltins, PyProtectedMember, PyUnresolvedReferences, PyUnusedLocal
    def __init__(
        self,
        context: Context | Registrator,
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
    def _assert_context(cls, context: Context | Registrator) -> Context | Registrator:
        from lori.core.register import RegistratorContext

        if context is None or not isinstance(context, (RegistratorContext, Registrator)):
            raise ResourceException(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    # noinspection PyShadowingBuiltins
    @classmethod
    def _build_id(
        cls,
        id: Optional[str] = None,
        key: Optional[str] = None,
        context: Optional[Context | Registrator] = None,
        configs: Optional[Configurations] = None,
    ) -> str:
        if configs is not None:
            if id is None:
                if configs.has_section(cls.SECTION) and "id" in configs[cls.SECTION]:
                    id = configs[cls.SECTION]["id"]
                elif "id" in configs:
                    id = configs["id"]
            if key is None:
                if configs.has_section(cls.SECTION) and "key" in configs[cls.SECTION]:
                    key = configs[cls.SECTION]["key"]
                elif "key" in configs:
                    key = configs["key"]
                else:
                    key = validate_key("_".join(os.path.splitext(configs.name)[:-1]))
        if key is None:
            raise ResourceException(f"Unable to build '{cls.__name__}' ID")
        if id is None and context is not None and isinstance(context, Registrator):
            id = f"{context.id}.{key}"
        else:
            id = key
        return id

    @classmethod
    def _build_key(cls, key: Optional[str], configs: Optional[Configurations]) -> str:
        if configs is not None:
            if configs.has_section(cls.SECTION) and "key" in configs[cls.SECTION]:
                key = configs[cls.SECTION]["key"]
            elif "key" in configs:
                key = configs["key"]
            else:
                if configs.has_section(cls.SECTION) and "name" in configs[cls.SECTION]:
                    key = validate_key(configs[cls.SECTION]["name"])
                elif "name" in configs:
                    key = validate_key(configs["name"])
                elif key is None:
                    key = validate_key("_".join(os.path.splitext(configs.name)[:-1]))
        if key is None:
            raise ResourceException(f"Unable to build '{cls.__name__}' Key")
        return key

    @classmethod
    def _build_name(cls, name: Optional[str], configs: Optional[Configurations]) -> str:
        if configs is not None:
            if configs.has_section(cls.SECTION) and "name" in configs[cls.SECTION]:
                name = configs[cls.SECTION]["name"]
            elif "name" in configs:
                name = configs["name"]
        return name

    @classmethod
    def _build_defaults(cls, configs: Configurations, includes: Optional[Collection[str]] = ()) -> Dict[str, Any]:
        return {k: deepcopy(v) for k, v in configs.items() if k in cls.INCLUDES or k in includes}

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
        except (ResourceException, AttributeError):
            # Abstract properties are not yet instanced
            pass

        def is_from_framework(value: Any) -> bool:
            from lori.core import Configurator, Context, Resource, Resources

            if isinstance(value, (Context, Resource, Resources, Configurator, Entity)):
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
    def context(self) -> Context:
        return self.__context

    # noinspection PyProtectedMember, PyTypeChecker, PyShadowingBuiltins
    def duplicate(self, context: Optional[Context | Registrator] = None, **changes):
        if context is None:
            context = self.__context
        return super().duplicate(context=context, **changes)
