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
    # noinspection PyProtectedMember
    def __init__(self, component) -> None:
        from loris import Component, ComponentException
        from loris.data.context import DataContext
        super().__init__(get_context(component.context, DataContext))

        if component is None or not isinstance(component, Component):
            raise ComponentException(f"Invalid component: {None if component is None else type(component)}")
        self.__component = component
        if not self.__component.configs.has_section(self.SECTION):
            self.__component.configs._add_section(self.SECTION, {})

    # noinspection PyShadowingNames, PyArgumentList
    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        connectors = RegistratorContext.__getattribute__(self, f"_{RegistratorContext.__name__}__map")
        if attr in connectors.keys():
            return connectors[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no connector '{attr}'")

    # noinspection PyProtectedMember
    def _add(self, connector: Connector) -> None:
        super()._set(connector.id, connector)
        self.context.connector._set(connector.uuid, connector)

    # noinspection PyProtectedMember
    def _update(self, context: Context, configs: Configurations) -> None:
        value = self._new(context, configs)
        if value.id in self:
            self._get(value.id).configs.update(configs)
        else:
            self._add(value)

    @property
    def configs(self) -> Configurations:
        return self.__component.configs.get_section(self.SECTION)

    # noinspection PyProtectedMember
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self.context.connectors._load_sections(self.__component, configs)
