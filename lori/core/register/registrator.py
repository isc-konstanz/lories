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
from typing import Any, Dict, Optional

from lori.core import Configurations, Configurator, Context, Identifier, ResourceException
from lori.util import get_context, validate_key


class Registrator(Configurator, Identifier):
    SECTION: str = "registration"

    __context: Context

    # noinspection PyProtectedMember, PyShadowingBuiltins, PyUnusedLocal
    def __init__(
        self,
        context: Registrator | Context = None,
        configs: Optional[Configurations] = None,
        id: Optional[str] = None,
        key: Optional[str] = None,
        name: Optional[str] = None,
        **kwargs,
    ) -> None:
        key = self._build_key(key, configs=configs)
        id = self._build_id(id, key, configs=configs, context=context)

        context = self._assert_context(context)
        if configs is None:
            configs = Configurations(f"{key}.conf", context.configs.dirs)
        super().__init__(
            configs=configs,
            name=self._build_name(name, configs=configs),
            id=id,
            key=key,
            **kwargs,
        )
        self.__context = context

    @classmethod
    def _assert_context(cls, context: Registrator | Context) -> Context:
        from lori.core.register import RegistratorContext

        if context is None or not isinstance(context, (Registrator, RegistratorContext)):
            raise ResourceException(f"Invalid '{cls.__name__}' context: {type(context)}")
        return get_context(context, RegistratorContext)

    # noinspection PyShadowingBuiltins
    @classmethod
    def _assert_id(cls, id: Optional[str], key: Optional[str]) -> str:
        id = super()._assert_id(id, key)
        if len(id.split(".")) <= 1:
            raise ResourceException(f"Missing context in '{cls.__name__}' id: {id}")
        return id

    # noinspection PyShadowingBuiltins
    @classmethod
    def _build_id(
        cls,
        id: Optional[str],
        key: Optional[str],
        configs: Optional[Configurations],
        context: Context,
    ) -> str:
        if configs is not None:
            if configs.has_section(cls.SECTION) and "id" in configs[cls.SECTION]:
                id = configs[cls.SECTION]["id"]
            elif "id" in configs:
                id = configs["id"]
        if id is None:
            id = f"{get_context(context, Identifier).id}.{key}"
        return id

    @classmethod
    def _build_key(cls, key: str, configs: Optional[Configurations]) -> str:
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
        return key

    @classmethod
    def _build_name(cls, name: Optional[str], configs: Optional[Configurations]) -> str:
        if configs is not None:
            if configs.has_section(cls.SECTION) and "name" in configs[cls.SECTION]:
                name = configs[cls.SECTION]["name"]
            elif "name" in configs:
                name = configs["name"]
        return name

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

            if isinstance(value, (Context, Resource, Resources, Configurator, Identifier)):
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
