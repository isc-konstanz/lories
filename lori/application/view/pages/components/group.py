# -*- coding: utf-8 -*-
"""
lori.application.view.pages.components.group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Collection, Dict, Generic, Optional, Type, TypeVar

import dash_bootstrap_components as dbc
from dash import html

from lori import Channels, Component
from lori.application.view.pages import PageGroup, PageLayout
from lori.application.view.pages.components import ComponentPage

ComponentType = TypeVar("ComponentType", bound=Component)
ChildrenType = TypeVar("ChildrenType", bound=Dict[str, Type[ComponentPage]])


# noinspection PyShadowingBuiltins
class ComponentGroup(PageGroup[ComponentPage], ComponentPage[ComponentType], Generic[ComponentType]):
    def __init__(self, component: ComponentType, *args, **kwargs) -> None:
        super().__init__(component=component, *args, **kwargs)

    def _at_create_layout(self, layout: PageLayout) -> None:
        super()._at_create_layout(layout)
        for page in self:
            page.create_layout(page.layout)

    def _create_data_layout(self, layout: PageLayout, channels: Channels, title: Optional[str] = "Data") -> None:
        if len(channels) > 0:
            data = []
            if title is not None:
                data.append(html.H5(f"{title}:"))
            data.append(self._build_data(channels))
            layout.append(dbc.Row(dbc.Col(dbc.Card(dbc.CardBody(data)))))

    def get_page(self, _page: str | Component) -> Optional[ComponentPage]:
        if isinstance(_page, Component):
            _page = f"{self.id}-{self._encode_id(_page.key)}"
        elif not isinstance(_page, str):
            raise TypeError(f"Invalid page type '{type(_page)}'")
        return super().get_page(_page)

    def get_pages(self, *_pages: str | Type[Component | ComponentPage]) -> Collection[ComponentPage]:
        pages = []
        for _page in _pages:
            for page in self._pages:
                if isinstance(_page, str):
                    if page.id == _page:
                        pages.append(page)
                elif isinstance(_page, type):
                    if isinstance(page, _page) or isinstance(page._component, _page):
                        pages.append(page)
                else:
                    raise TypeError(f"Invalid page type '{type(_page)}'")
        return pages
