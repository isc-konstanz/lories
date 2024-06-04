# -*- coding: utf-8 -*-
"""
    loris.connectors.registry
    ~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations

from loris.connectors import Connector, ConnectorException

types = {}


# noinspection PyShadowingBuiltins
def register(cls: type, type: str, factory: callable = None, replace: bool = False) -> None:
    if type in types and not replace:
        raise ConnectorException(f"Connector '{type}' does already exist: {types[type].name}")
    types[type] = ConnectorRegistration(cls, type, factory)


# noinspection PyShadowingBuiltins
class ConnectorRegistration:
    _class: type
    _factory: callable

    type: str

    def __init__(self, cls: type, type: str, factory: callable = None):
        self.type = type
        self._class = cls
        self._factory = cls if factory is None else factory

    @property
    def name(self):
        return self._class.__name__

    def initialize(self, *args, **kwargs) -> Connector:
        factory = self._factory
        if factory is None:
            factory = self._class
        elif not callable(factory):
            raise ConnectorException(f"Invalid component initialization function: {factory}")

        component = factory(*args, **kwargs)

        if not isinstance(component, Connector):
            raise ConnectorException(f"Invalid component type: {type(component)}")

        return component
