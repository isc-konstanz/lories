# -*- coding: utf-8 -*-
"""
loris.connectors.access
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from loris.connectors import Connector, ConnectorContext
from loris.core import Configurations, Context, RegistratorContext
from loris.util import get_context


class ConnectorAccess(ConnectorContext):
    _created: bool = False

    def __init__(self, component) -> None:
        from loris import Component, ComponentException
        from loris.data.context import DataContext

        super().__init__(get_context(component.context, DataContext))
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
