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

    def create_layout(self, layout: PageLayout) -> None:
        super().create_layout(layout)
        layout.menu = dbc.NavItem(dbc.NavLink(self.name, href=self.path))
        layout.card = html.Div(
            [
                html.H4(self.name),
                dbc.Alert(f"This is a placeholder for the {self.name} card view", color="secondary"),
            ]
        )

        layout.append(
            dbc.ListGroup(
                [dbc.ListGroupItem(p.layout.card, href=p.path) for p in self._pages if p.layout.has_card_view()],
                flush=True,
            ),
        )
