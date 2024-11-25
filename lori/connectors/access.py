# -*- coding: utf-8 -*-
"""
lori.connectors.access
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori.connectors import Connector, ConnectorContext
from lori.core import Configurations, Context, Registrator, RegistratorContext, ResourceException
from lori.util import get_context


# noinspection PyProtectedMember
class ConnectorAccess(ConnectorContext):
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
        super()._load(self.__component, configs, **kwargs)

    # noinspection PyArgumentList
    def __contains__(self, __connector: str | Connector) -> bool:
        connectors = Context.__getattribute__(self, f"_{Context.__name__}__map")
        if isinstance(__connector, str):
            if not len(__connector.split(".")) > 1:
                __connector = f"{self.__component.id}.{__connector}"
            return __connector in connectors.keys()
        if isinstance(__connector, Connector):
            return __connector in connectors.values()
        return False

    # noinspection PyArgumentList
    def __getattr__(self, attr):
        connectors = Context.__getattribute__(self, f"_{Context.__name__}__map")
        connectors_by_key = {c.key: c for c in connectors.values()}
        if attr in connectors_by_key:
            return connectors_by_key[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no connector '{attr}'")

    def __delitem__(self, __uid: str) -> None:
        if not len(__uid.split(".")) > 1:
            __uid = f"{self.__component.id}.{__uid}"
        del self.context[__uid]
        del self[__uid]

    # noinspection PyUnresolvedReferences
    def add(self, connector: Connector | Configurations) -> None:
        if isinstance(connector, Connector):
            self._add(connector)
        elif isinstance(connector, Configurations):
            # TODO: Implement DataAccess like configuration appending
            raise NotImplementedError("Not yet implemented")
        else:
            raise ResourceException(f"Invalid connector type: {type(connector)}")

    # noinspection PyShadowingBuiltins
    def _set(self, id: str, connector: Connector) -> None:
        self.context.connectors._set(id, connector)
        super()._set(id, connector)

    # noinspection PyShadowingBuiltins
    def _new(self, context: Context, configs: Configurations) -> Connector:
        return self.context.connectors._new(context, configs)

    # noinspection PyShadowingBuiltins
    def _get(self, id: str) -> Connector:
        if not len(id.split(".")) > 1:
            id = f"{self.__component.id}.{id}"
        return super()._get(id)
