# -*- coding: utf-8 -*-
"""
loris.app.view.pages.components.registry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Collection, Generic, Optional, Type, TypeVar

from loris.app.view.pages import Page
from loris.app.view.pages.components import ComponentGroup, ComponentPage
from loris.components import Component
from loris.core import ResourceException
from loris.util import parse_key, parse_name

C = TypeVar("C", bound=Component)
CP = TypeVar("CP", bound=ComponentPage)
CG = TypeVar("CG", bound=ComponentGroup)

P = TypeVar("P", bound=Page)


# noinspection PyShadowingBuiltins
class Registration(ABC, Generic[P]):
    _class: Type[P]
    _factory: callable

    def __init__(
        self,
        cls: Type[P],
        factory: Optional[Callable] = None,
    ):
        self._class = cls
        self._factory = cls if factory is None else factory

    @abstractmethod
    def has_type(self, *args) -> bool:
        pass

    def initialize(self, *args, **kwargs) -> P:
        factory = self._factory
        if factory is None:
            factory = self._class
        elif not callable(factory):
            raise ResourceException(f"Invalid registration initialization function: {factory}")

        return factory(*args, **kwargs)


# noinspection PyShadowingBuiltins
class PageRegistration(Registration[CP]):
    type: Type[C]

    def __init__(
        self,
        cls: Type[CP],
        type: Type[C],
        factory: Optional[Callable] = None,
    ):
        super().__init__(cls, factory)
        self.type = type

    def has_type(self, type: Type[CP]) -> bool:
        return issubclass(type, self.type)


# noinspection PyShadowingBuiltins
class GroupRegistration(Registration[CG]):
    types: Collection[Type[C]]

    id: str
    name: str

    def __init__(
        self,
        cls: Type[CG],
        *types: Type[C],
        id: Optional[str] = None,
        name: Optional[str] = None,
        factory: Optional[Callable] = None,
    ):
        super().__init__(cls, factory)
        self.types = types

        if name is None:
            if len(types) > 1:
                raise ValueError(f"Ambiguous ID for {len(types)} types of class: {cls.__init__()}")
            name = parse_name(types[0].TYPE)
        self.name = name

        if id is None:
            id = parse_key(name)
        self.id = id

    def has_type(self, *types: Type[CG]) -> bool:
        _types = [t for t in self.types if any(issubclass(_t, t) for _t in types)]
        return len(_types) > 0


# noinspection PyShadowingBuiltins
class ComponentRegistry:
    pages: Collection[PageRegistration]
    groups: Collection[GroupRegistration]

    def __init__(self) -> None:
        self.pages: list[PageRegistration] = []
        self.groups: list[GroupRegistration] = []

    # noinspection PyTypeChecker, PyUnresolvedReferences
    def register_page(
        self,
        cls: Type[CP],
        type: Type[C],
        factory: Optional[Callable] = None,
        replace: bool = False,
    ) -> None:
        if not issubclass(cls, ComponentPage):
            raise ValueError("Can only register ComponentPage types")
        existing = self._get_pages(type)
        if len(existing) > 0:
            if replace:
                for page in existing:
                    self.pages.remove(page)
            else:
                raise ResourceException(f"Registration for '{type.TYPE}' does already exist: " + ", ".join(existing))
        self.pages.append(PageRegistration(cls, type, factory=factory))

    # noinspection PyTypeChecker, PyUnresolvedReferences
    def register_group(
        self,
        cls: Type[CG],
        *types: Type[C],
        name: Optional[str] = None,
        factory: Optional[Callable] = None,
        replace: bool = False,
    ) -> None:
        if not issubclass(cls, ComponentGroup):
            raise ValueError("Can only register ComponentGroup types")
        existing = self._get_groups(*types)
        if len(existing) > 0:
            if replace:
                for group in existing:
                    self.groups.remove(group)
            else:
                raise ResourceException("Registration does already exist for types: " + ", ".join(existing))
        self.groups.append(GroupRegistration(cls, *types, name=name, factory=factory))

    def has_page(self, *types: Type[C]) -> bool:
        return len(self._get_pages(*types)) > 0

    def _get_pages(self, *types: Type[C]) -> Collection[PageRegistration]:
        return [p for p in self.pages if p.has_type(*types)]

    def get_page(self, type: Type[C]) -> PageRegistration:
        for page in self.pages:
            if page.has_type(type):
                return page
        raise ValueError(f"Registration '{type}' does not exist")

    def has_group(self, *types: Type[C]) -> bool:
        return len(self._get_groups(*types)) > 0

    def _get_groups(self, *types: Type[C]) -> Collection[GroupRegistration]:
        return [g for g in self.groups if g.has_type(*types)]

    def get_group(self, type: Type[C]) -> GroupRegistration:
        for group in self.groups:
            if group.has_type(type):
                return group
        raise ValueError(f"Registration '{type}' does not exist")
