# -*- coding: utf-8 -*-
"""
lori.application.view.pages.view
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from functools import wraps
from typing import Any, Callable, Dict, Optional, Type, TypeVar

import dash_bootstrap_components as dbc
from dash import html

from lori import Configurator
from lori.application.view.pages import Page, PageFooter, PageGroup, PageHeader, PageLayout, PageRegistry
from lori.components import Component, ComponentContext
from lori.system import System
from lori.util import validate_key

PageType = TypeVar("PageType", bound=Page)

ComponentType = TypeVar("ComponentType", bound=Component)
ChildrenType = TypeVar("ChildrenType", bound=Dict[str, Type[Page]])

registry = PageRegistry()


# noinspection PyShadowingBuiltins
def register_component_page(
    *types: Type[ComponentType],
    factory: Optional[Callable] = None,
    children: Optional[ChildrenType] = None,
    replace: bool = False,
) -> Callable[[Type[PageType]], Type[PageType]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[PageType]) -> Type[PageType]:
        registry.register_page(cls, *types, factory=factory, children=children, replace=replace)
        return cls

    return _register


def register_component_group(
    *types: Type[ComponentType],
    key: Optional[str] = None,
    name: Optional[str] = None,
    factory: Optional[Callable] = None,
    custom: bool = False,
    replace: bool = False,
) -> Callable[[Type[PageType]], Type[PageType]]:
    if name is None:
        name = types[0].__name__
    if key is None:
        key = validate_key(name)

    # noinspection PyShadowingBuiltins
    def _register(cls: Type[PageType]) -> Type[PageType]:
        if custom:
            if not issubclass(cls, PageGroup):
                raise ValueError(f"Unknown page group type: {cls.__name__}")
            type = cls
        else:
            type = PageGroup

        registry.register_group(type, *types, key=key, name=name, factory=factory, replace=replace)
        return cls

    return _register


class View(PageGroup):
    groups: Dict[str, PageGroup]

    # noinspection PyShadowingBuiltins
    def __init__(self, id: str, header: PageHeader, footer: PageFooter, *args, **kwargs) -> None:
        super().__init__(id=f"{id}-view", name="View", path="/", *args, **kwargs)
        self.header = header
        self.footer = footer

        self.groups = {}

    @property
    def key(self) -> str:
        return "view"

    @property
    def path(self) -> str:
        return "/"

    # noinspection PyProtectedMember
    def create_layout(self, layout: PageLayout) -> None:
        if all(isinstance(p._component, System) for p in self):
            label = "Systems"
        else:
            label = "Select"

        layout.menu = dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem(label, header=True),
                *[dbc.DropdownMenuItem(p.name, href=p.path) for p in self],
            ],
            nav=True,
            in_navbar=True,
            label=label,
        )

        layout.container.class_name = "card-container card-focus"
        layout.container.children = []
        for page in self._pages:
            page_cards = []
            if isinstance(page, PageGroup):
                layout.append(
                    dbc.Row(dbc.Col(html.A(html.H4(f"{page.name}:"), className="card-header", href=page.path)))
                )
                for page_group in page:
                    if page_group.layout.has_card_items():
                        page_cards.append(page_group.layout.card)
            else:
                if page.layout.has_card_items():
                    page_cards.append(page.layout.card)

            layout.append(dbc.Row([dbc.Col(card, width="auto") for card in page_cards]))

    # noinspection PyTypeChecker, PyUnresolvedReferences
    @wraps(create_layout, updated=())
    def _do_create_layout(self, *args, **kwargs) -> None:
        for page in self:
            page._do_create_layout(*args, **kwargs)
        for group in self.groups.values():
            group._do_create_layout(*args, **kwargs)
            group_layout = group.layout
            if group_layout.has_menu_item():
                self.header.menu.append(group_layout.menu)

        super()._do_create_layout(*args, **kwargs)
        self.header.menu.insert(0, self.layout.menu)

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def _do_create_pages(self, components: ComponentContext) -> None:
        systems = [s for s in components.filter(lambda c: isinstance(c, System))]
        for system in systems:
            system_page = self._new_page(self, system)
            for component in system.values():
                self._new_page(system_page, component)

        for component in components.filter(lambda c: all(c != s and c not in s for s in systems)):
            self._new_page(self, component)

    # noinspection PyUnresolvedReferences
    def _new_page(self, view: PageGroup, component: Any) -> Optional[Page]:
        if isinstance(component, Configurator) and not component.is_enabled():
            self._logger.debug(f"Skipping page creation for disabled {type(component).__name__} '{component.id}'")
            return

        _type = type(component)
        if not registry.has_page(_type):
            return

        registration = registry.get_page(_type)
        page = registration.initialize(
            component=component,
            group=view,
        )
        if page is not None:
            group = self._get_group(component)
            if group is not None:
                group.append(page)
                page.group = group
            view.append(page)
        return page

    def _new_group(self, component: Any) -> Optional[PageGroup]:
        _type = type(component)
        if not registry.has_group(_type):
            return

        registration = registry.get_group(_type)
        group = registration.initialize(
            id=f"{self.id}-{registration.key}",
            key=registration.key,
            name=registration.name,
        )
        if group is not None:
            self.groups[registration.key] = group
        return group

    def _get_group(self, component: Any) -> Optional[PageGroup]:
        _type = type(component)
        if not registry.has_group(_type):
            return
        group = self.groups.get(registry.get_group(_type).key, None)
        if group is None:
            group = self._new_group(component)
        return group

    def _do_register(self) -> None:
        groups = self.groups.values()
        for page in [p for p in self if p not in groups]:
            if page.is_active():
                page._do_register()
        for group in groups:
            group._do_register()
        return super()._do_register()
