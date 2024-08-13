# -*- coding: utf-8 -*-
"""
loris.app.view.pages.components.group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Collection, Generic, TypeVar

import dash_bootstrap_components as dbc
from dash import html

import loris
from loris.app.view.pages import PageGroup, PageLayout
from loris.app.view.pages.components import ComponentPage

C = TypeVar("C", bound=loris.Component)


class ComponentGroup(PageGroup[ComponentPage[C]], Generic[C]):
    # noinspection PyProtectedMember
    @property
    def components(self) -> Collection[C]:
        return [p._component for p in self]

    def create_layout(self) -> PageLayout:
        return PageLayout(
            content=self._create_content_layout(),
            focus=self._create_focus_layout(),
            menu=self._create_menu_item()
        )

    def _create_content_layout(self) -> html.Div:
        return html.Div(
            [
                html.H4(f"{self.name}"),
                html.Hr(),
                dbc.ListGroup(
                    [dbc.ListGroupItem(p.layout.focus, href=p.path) for p in self._pages if p.layout.has_focus_view()],
                    flush=True,
                ),
            ]
        )

    def _create_focus_layout(self) -> html.Div:
        return html.Div(
            [
                html.H4(self.name),
                dbc.Alert(f"This is a placeholder for the {self.name} focus view", color="secondary"),
            ]
        )

    def _create_menu_item(self) -> dbc.NavItem:
        return dbc.NavItem(dbc.NavLink(self.name, href=self.path))
