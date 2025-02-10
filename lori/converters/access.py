# -*- coding: utf-8 -*-
"""
lori.converters.access
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori.converters import Converter, ConverterContext
from lori.core import Configurations, Context, Registrator, RegistratorContext, ResourceException
from lori.util import get_context


# noinspection PyProtectedMember
class ConverterAccess(ConverterContext):
    def __init__(self, component) -> None:
        self.__component = self._assert_component(component)
        super().__init__(component.context, component.configs.get_sections([self.SECTION], ensure_exists=True))

    @classmethod
    def _assert_component(cls, component):
        from lori.components import Component

        if component is None or not isinstance(component, (Component, Context)):
            raise ResourceException(f"Invalid '{cls.__name__}' component: {type(component)}")
        return component

    @classmethod
    def _assert_context(cls, context: Context) -> Context:
        from lori.components import Component
        from lori.data.manager import DataManager

        if context is None or not isinstance(context, (Component, Context)):
            raise ResourceException(f"Invalid '{cls.__name__}' context: {type(context)}")
        return get_context(context, DataManager)

    @classmethod
    def _assert_configs(cls, configs: Configurations) -> Configurations:
        configs = super()._assert_configs(configs)
        if configs is None:
            raise ResourceException(f"Invalid '{cls.__name__}' NoneType configurations")
        return configs

    # noinspection PyUnusedLocal
    def _load(
        self,
        context: Registrator | RegistratorContext,
        configs: Configurations,
        **kwargs,
    ) -> None:
        super()._load(self.__component, configs)

    # noinspection PyArgumentList
    def __contains__(self, __converter: str | Converter) -> bool:
        converters = Context.__getattribute__(self, f"_{Context.__name__}__map")
        if isinstance(__converter, str):
            if not len(__converter.split(".")) > 1:
                __converter = f"{self.__component.id}.{__converter}"
            return __converter in converters.keys()
        if isinstance(__converter, Converter):
            return __converter in converters.values()
        return False

    # noinspection PyArgumentList
    def __getattr__(self, attr):
        converters = Context.__getattribute__(self, f"_{Context.__name__}__map")
        converters_by_key = {c.key: c for c in converters.values()}
        if attr in converters_by_key:
            return converters_by_key[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no converter '{attr}'")

    def __delitem__(self, __uid: str) -> None:
        if not len(__uid.split(".")) > 1:
            __uid = f"{self.__component.id}.{__uid}"
        del self.context[__uid]
        del self[__uid]

    # noinspection PyUnresolvedReferences
    def add(self, converter: Converter | Configurations) -> None:
        if isinstance(converter, Converter):
            self._add(converter)
        elif isinstance(converter, Configurations):
            # TODO: Implement DataAccess like configuration appending
            raise NotImplementedError("Not yet implemented")
        else:
            raise ResourceException(f"Invalid converter type: {type(converter)}")

    # noinspection PyShadowingBuiltins
    def _set(self, id: str, converter: Converter) -> None:
        self.context.converters._set(id, converter)
        super()._set(id, converter)

    # noinspection PyShadowingBuiltins
    def _new(self, context: Context, configs: Configurations) -> Converter:
        return self.context.converters._new(context, configs)

    # noinspection PyShadowingBuiltins
    def _get(self, id: str) -> Converter:
        if not len(id.split(".")) > 1:
            id = f"{self.__component.id}.{id}"
        return super()._get(id)
