# -*- coding: utf-8 -*-
"""
lori.application.view.pages.components.registry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Collection, Dict, Generic, List, Optional, Type, TypeVar

from lori.application.view.pages import Page, PageGroup
from lori.core import ResourceException
from lori.util import validate_key

PageType = TypeVar("PageType", bound=Page)
GroupType = TypeVar("GroupType", bound=PageGroup)

ChildrenType = TypeVar("ChildrenType", bound=Dict[str, Type[Page]])


# noinspection PyShadowingBuiltins
class _PageRegistration(ABC, Generic[PageType]):
    _class: Type[PageType]
    _factory: callable

    _kwargs: Dict[str, Any]

    def __init__(
        self,
        cls: Type[PageType],
        factory: Optional[Callable] = None,
        **kwargs,
    ):
        self._class = cls
        self._factory = cls if factory is None else factory
        self._kwargs = kwargs

    @abstractmethod
    def has_type(self, *args) -> bool:
        pass

    def initialize(self, *args, **kwargs) -> PageType:
        factory = self._factory
        if factory is None:
            factory = self._class
        elif not callable(factory):
            raise ResourceException(f"Invalid registration initialization function: {factory}")

        return factory(*args, **kwargs, **self._kwargs)


# noinspection PyShadowingBuiltins
class PageRegistration(_PageRegistration[PageType]):
    type: Type

    def __init__(
        self,
        cls: Type[PageType],
        type: Type,
        factory: Optional[Callable] = None,
        **kwargs,
    ):
        super().__init__(cls, factory, **kwargs)
        self.type = type

    def has_type(self, type: Type) -> bool:
        return issubclass(type, self.type)


# noinspection PyShadowingBuiltins
class GroupRegistration(_PageRegistration[GroupType]):
    types: List[Type]

    key: str
    name: str

    def __init__(
        self,
        cls: Type[GroupType],
        *types: Type,
        key: Optional[str] = None,
        name: Optional[str] = None,
        factory: Optional[Callable] = None,
    ):
        super().__init__(cls, factory)
        self.types = list(types)

        if name is None:
            if len(types) > 1:
                raise ValueError(f"Ambiguous ID for {len(types)} types of class: {cls.__init__()}")
            name = types[0].__name__
        self.name = name

        if key is None:
            key = validate_key(name)
        self.key = key

    def add_type(self, type: Type) -> None:
        self.types.append(type)

    def has_type(self, *types: Type) -> bool:
        _types = [t for t in self.types if any(issubclass(_t, t) for _t in types)]
        return len(_types) > 0


# noinspection PyShadowingBuiltins
class PageRegistry:
    pages: Collection[PageRegistration]
    groups: Collection[GroupRegistration]

    def __init__(self) -> None:
        self.pages: list[PageRegistration] = []
        self.groups: list[GroupRegistration] = []

    # noinspection PyTypeChecker, PyProtectedMember
    def register_page(
        self,
        cls: Type[GroupType],
        type: Type,
        factory: Optional[Callable] = None,
        children: Optional[ChildrenType] = None,
        replace: bool = False,
    ) -> None:
        if not issubclass(cls, Page):
            raise ValueError("Can only register Page types")
        existing = self._get_pages(type)
        if len(existing) > 0:
            if replace:
                for page in existing:
                    self.pages.remove(page)
            else:
                raise ResourceException(
                    f"Registration for '{type}' does already exist: " ", ".join(p._class.__name__ for p in existing)
                )
        self.pages.append(PageRegistration(cls, type, children=children, factory=factory))

    # noinspection PyTypeChecker, PyProtectedMember, PyUnresolvedReferences
    def register_group(
        self,
        cls: Type[GroupType],
        *types: Type,
        key: Optional[str] = None,
        name: Optional[str] = None,
        factory: Optional[Callable] = None,
        replace: bool = False,
    ) -> None:
        if not issubclass(cls, PageGroup):
            raise ValueError("Can only register PageGroup types")
        existing = self._get_groups(*types)
        if len(existing) > 0:
            if replace:
                for group in existing:
                    self.groups.remove(group)
            else:
                raise ResourceException(
                    f"Registration for types "
                    f"'{', '.join(t.__name__ for t in types)}' does already exist: "
                    ", ".join(p._class.__name__ for p in existing)
                )
        self.groups.append(GroupRegistration(cls, *types, key=key, name=name, factory=factory))

    def has_page(self, *types: Type) -> bool:
        return len(self._get_pages(*types)) > 0

    def _get_pages(self, *types: Type) -> Collection[PageRegistration]:
        return [p for p in self.pages if p.has_type(*types)]

    def get_page(self, type: Type) -> PageRegistration:
        for page in self.pages:
            if page.has_type(type):
                return page
        raise ValueError(f"Registration '{type}' does not exist")

    def has_group(self, *types: Type) -> bool:
        return len(self._get_groups(*types)) > 0

    def _get_groups(self, *types: Type) -> Collection[GroupRegistration]:
        return [g for g in self.groups if g.has_type(*types)]

    def get_group(self, type: Type) -> GroupRegistration:
        for group in self.groups:
            if group.has_type(type):
                return group
        raise ValueError(f"Registration '{type}' does not exist")
