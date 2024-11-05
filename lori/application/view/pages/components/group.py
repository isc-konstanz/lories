# -*- coding: utf-8 -*-
"""
lori.application.view.pages.components.group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Collection, Generic, TypeVar

import dash_bootstrap_components as dbc
from dash import html

import lori
from lori.application.view.pages import PageGroup, PageLayout
from lori.application.view.pages.components import ComponentPage

C = TypeVar("C", bound=lori.Component)


class ComponentGroup(PageGroup[ComponentPage[C]], Generic[C]):
    # noinspection PyProtectedMember
    @property
    def components(self) -> Collection[C]:
        return [p._component for p in self]

    def create_layout(self, layout: PageLayout) -> None:
        layout.container.class_name = "card-container"

        layout.menu = dbc.NavItem(dbc.NavLink(self.name, href=self.path))
        layout.card.add_title(self.name)
        layout.card.add_footer(href=self.path)

        layout.append(dbc.Row(dbc.Col(html.H4(f"{self.name}:"))))

        for page in self.sort():
            if page.layout.has_card_items():
                layout.append(dbc.Row(dbc.Col(page.layout.card, width="auto")))
