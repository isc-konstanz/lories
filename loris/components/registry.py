# -*- coding: utf-8 -*-
"""
    loris._components.registration
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from typing import List

from loris.components import Component, ComponentException

types = {}


# noinspection PyShadowingBuiltins
def register(cls: type, type: str, *alias: str, factory: callable = None, replace: bool = False) -> None:
    if type in types and not replace:
        raise ComponentException(f"Component \"{type}\" does already exist: {types[type].name}")
    types[type] = ComponentRegistration(cls, type, *alias, factory)


# noinspection PyShadowingBuiltins
class ComponentRegistration:

    _class: type
    _factory: callable

    type: str
    alias: List[str]

    def __init__(self, cls: type, type: str, *alias: str, factory: callable = None):
        self.type = type
        self._class = cls
        self._factory = cls if factory is None else factory
        self.alias = list(a for a in alias if a is not None)

    @property
    def name(self):
        return self._class.__name__

    def is_alias(self, type: str) -> bool:
        return any(type.startswith(a) for a in self.alias)

    def initialize(self, *args, **kwargs) -> Component:
        factory = self._factory
        if factory is None:
            factory = self._class
        elif not callable(factory):
            raise ComponentException(f"Invalid component initialization function: {factory}")

        component = factory(*args, **kwargs)

        if not isinstance(component, Component):
            raise ComponentException(f"Invalid component type: {type(component)}")

        return component
