# -*- coding: utf-8 -*-
"""
lori.connectors.access
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori.connectors import Connector, ConnectorContext
from lori.core import Configurations, Context, RegistratorContext
from lori.util import get_context


class ConnectorAccess(ConnectorContext):
    _created: bool = False

    def __init__(self, component) -> None:
        from lori import Component, ComponentException
        from lori.data.context import DataContext

        context = get_context(component.context, DataContext)
        configs = component.configs.get_section(self.SECTION, ensure_exists=True)
        super().__init__(context, configs)
        if component is None or not isinstance(component, Component):
            raise ComponentException(f"Invalid component: {None if component is None else type(component)}")
        self.__component = component

    # noinspection PyShadowingNames, PyArgumentList
    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        connectors = RegistratorContext.__getattribute__(self, f"_{RegistratorContext.__name__}__map")
        if attr in connectors.keys():
            return connectors[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no connector '{attr}'")

    # noinspection PyProtectedMember
    def create(self) -> None:
        self._add(*self.context.connectors._load_sections(self.__component, self.configs))
        self._created = True

    def is_created(self) -> bool:
        return self._created

    # noinspection PyProtectedMember
    def _add(self, *connectors: Connector) -> None:
        for connector in connectors:
            super()._set(connector.key, connector)
            self.context.connectors._set(connector.id, connector)

    # noinspection PyProtectedMember
    def _update(self, context: Context, configs: Configurations) -> Connector:
        connector = self._new(context, configs)
        if connector.key in self:
            self._get(connector.key).configs.update(configs)
        else:
            self._add(connector)
        return connector
