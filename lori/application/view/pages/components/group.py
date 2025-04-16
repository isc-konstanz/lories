# -*- coding: utf-8 -*-
"""
lori.application.view.pages.components.group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Dict, Generic, Optional, Type, TypeVar

import dash_bootstrap_components as dbc
from dash import html

from lori import Component
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

    def _create_data_layout(self, layout: PageLayout, title: Optional[str] = "Data") -> None:
        if len(self.data.channels) > 0:
            data = []
            if title is not None:
                data.append(html.H5(f"{title}:"))
            data.append(self._build_data())
            layout.append(dbc.Row(dbc.Col(dbc.Card(dbc.CardBody(data)))))
