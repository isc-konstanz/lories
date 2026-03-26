# -*- coding: utf-8 -*-
"""
lories.application.view.pages.view
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence, Type, TypeVar

import dash_bootstrap_components as dbc
from dash import html

from lories.application.view.pages import Page, PageFooter, PageGroup, PageHeader, PageLayout, PageRegistry
from lories.application.view.pages.connectors import ConnectorPage
from lories.components import Component, ComponentContext
from lories.connectors import ConnectorContext
from lories.system import System
from lories.util import validate_key

P = TypeVar("P", bound=Page)
C = TypeVar("C", bound=Component)

registry = PageRegistry()


# noinspection PyShadowingBuiltins
def register_component_page(
    *types: Type[C],
    factory: Optional[Callable] = None,
    replace: bool = False,
) -> Callable[[Type[P]], Type[P]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[P]) -> Type[P]:
        registry.register_page(cls, *types, factory=factory, replace=replace)
        return cls

    return _register


def register_component_group(
    *types: Type[C],
    key: Optional[str] = None,
    name: Optional[str] = None,
    factory: Optional[Callable] = None,
    custom: bool = False,
    replace: bool = False,
) -> Callable[[Type[P]], Type[P]]:
    if name is None:
        name = types[0].__name__
    if key is None:
        key = validate_key(name)

    # noinspection PyShadowingBuiltins
    def _register(cls: Type[P]) -> Type[P]:
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
        super().__init__(id=f"{id}-view", key="view", name="View", *args, **kwargs)
        self.header = header
        self.footer = footer

        self.groups = {}
        self._connector_pages: List[ConnectorPage] = []
        self._connector_ids: set = set()

    @property
    def path(self) -> str:
        return "/"

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def create_pages(self, components: ComponentContext) -> None:
        for component in components.values():
            if isinstance(component, System) or not isinstance(component.context, Component):
                self._create_page(self, component)

    def create_connector_pages(self, connectors: ConnectorContext) -> None:
        for connector in connectors.values():
            if connector.id in self._connector_ids:
                continue
            self._connector_ids.add(connector.id)
            page = ConnectorPage(connector=connector)
            self._connector_pages.append(page)

    # noinspection PyUnresolvedReferences
    def _create_page(self, view: PageGroup, component: Component) -> Optional[Page]:
        if not component.is_enabled():
            self._logger.debug(f"Skipping page creation for disabled {type(component).__name__} '{component.id}'")
            return None

        _type = type(component)
        if registry.has_page(_type):
            registration = registry.get_page(_type)
            page = registration.initialize(
                component=component,
                group=view,
            )
        else:
            from lories.application.view.pages.components.page import ComponentPage
            page = ComponentPage(component=component, group=view)
        if page is not None:
            group = self._get_group(component)
            if group is not None:
                group.append(page)

            if isinstance(page, PageGroup):
                for page_component in page.components.values():
                    self._create_page(page, page_component)

            view.append(page)
        return page

    def _create_group(self, component: Any) -> Optional[PageGroup]:
        _type = type(component)
        if not registry.has_group(_type):
            return None

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
            return None
        group = self.groups.get(registry.get_group(_type).key, None)
        if group is None:
            group = self._create_group(component)
        return group

    # noinspection PyProtectedMember
    def create_layout(self, layout: PageLayout) -> None:
        label = "Systems"
        menu = [dbc.DropdownMenuItem("Select", header=True)]
        for page in self._pages:
            if page.key in ["auth", "login"]:
                continue
            _component = getattr(page, "_component", None)
            if _component is not None and not isinstance(_component, System):
                label = "Select"
            menu.append(dbc.DropdownMenuItem(page.name, href=page.path))

        layout.menu = dbc.DropdownMenu(children=menu, nav=True, in_navbar=True, label=label)
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
    def _at_create_layout(self, layout: PageLayout) -> None:
        super()._at_create_layout(layout)
        for page in self:
            page.create_layout(page.layout)
        for group in self.groups.values():
            group.create_layout(group.layout)
        for page in self._connector_pages:
            page.create_layout(page.layout)

    def _on_create_layout(self, layout: PageLayout) -> None:
        super()._on_create_layout(layout)
        self.header.menu.insert(0, layout.menu)

    def _at_register(self) -> None:
        super()._at_register()
        groups = self.groups.values()

        def _register(pages: Sequence[Page]) -> None:
            for page in pages:
                if page.is_created() and page not in groups:
                    page.register()
                if isinstance(page, PageGroup):
                    _register(page)

        _register(self)
        for group in groups:
            group.register()
        for page in self._connector_pages:
            if page.is_created():
                page.register()
