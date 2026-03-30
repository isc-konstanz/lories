# -*- coding: utf-8 -*-
"""
lories.application.view.pages.header
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from os import PathLike
from typing import List, Optional

from dash import html
from dash.development.base_component import Component
from dash_bootstrap_components import Button, Col, Input, Nav, Navbar, NavbarBrand, Row
from PIL import Image


class PageHeader:
    _brand: Component

    menu: List[Component]

    def __init__(self, name: str, logo: Optional[str | PathLike] = None) -> None:
        super().__init__()
        if logo is not None:
            self._brand = Row(
                [
                    Col(html.Img(src=Image.open(logo), height="30px")),
                    Col(NavbarBrand(name, className="ms-2")),
                ],
                align="center",
                className="g-0",
            )
        else:
            self._brand = NavbarBrand(name, className="ms-2")

        self.menu = []

    @property
    def searchbar(self) -> Row:
        return Row(
            [
                Col(Input(type="search", placeholder="Search")),
                Col(
                    Button("Search", color="primary", className="ms-2", n_clicks=0),
                    width="auto",
                ),
            ],
            className="g-0 ms-auto flex-nowrap mt-3 mt-md-0",
            align="center",
        )

    @property
    def navbar(self) -> Navbar:
        isc_logo = html.A(
            html.Img(src="/assets/isc_logo.png", height="40px"),
            href="https://www.isc-konstanz.de",
            target="_blank",
            className="ms-auto",
        )
        return Navbar(
            html.Div(
                [
                    html.A(self._brand, href="/", className="navbar-brand"),
                    Nav(self.menu, navbar=True),
                    isc_logo,
                ],
                className="container-fluid d-flex align-items-center",
            ),
            color="light",
            dark=False,
        )
