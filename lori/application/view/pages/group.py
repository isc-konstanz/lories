# -*- coding: utf-8 -*-
"""
lori.application.view.pages.group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import re
from collections.abc import MutableSequence, Sequence
from itertools import chain
from typing import Any, Collection, Generic, Iterator, List, Optional, Tuple, Type, TypeVar

import dash_bootstrap_components as dbc
from dash import html

import pandas as pd
from lori.application.view.pages import Page, PageLayout

P = TypeVar("P", bound=Page)


class PageGroup(Page, MutableSequence[P], Generic[P]):
    _pages: List[P]

    order: int = 1000

    # noinspection PyShadowingBuiltins
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._pages = []

    def __len__(self) -> int:
        return len(self._pages)

    def __iter__(self) -> Iterator[P]:
        return iter(self._pages)

    def __contains__(self, page: str | P) -> bool:
        return self.has_page(page)

    def __getitem__(self, index: int) -> P:
        return self._pages[index]

    def __setitem__(self, index: int, page: P) -> None:
        self._pages[index] = page

    def __delitem__(self, index: int) -> None:
        del self._pages[index]

    def insert(self, index: int, page: P) -> None:
        self._pages.insert(index, page)

    def has_page(self, _page: str | P) -> bool:
        if isinstance(_page, str):
            return len(self.get_page(_page)) > 0
        elif isinstance(_page, Page):
            return _page in self._pages
        raise TypeError(f"Invalid page type '{type(_page)}'")

    def get_page(self, _page: str) -> Optional[P]:
        for page in self._pages:
            if page.id == _page:
                return page
        return None

    def get_pages(self, *_pages: str | Type[P]) -> Collection[P]:
        pages = []
        for _page in _pages:
            for page in self._pages:
                if isinstance(_page, str):
                    if page.id == _page:
                        pages.append(page)
                elif isinstance(_page, type):
                    if isinstance(page, _page):
                        pages.append(page)
                else:
                    raise TypeError(f"Invalid page type '{type(_page)}'")
        return pages

    def sort(self) -> Sequence[Page]:
        def order(page: Page) -> Tuple[Any, ...]:
            elements = re.split(r"[^0-9A-Za-zäöüÄÖÜß]+", page.id)
            elements = list(chain(*[re.findall(r"\D+|\d+", t) for t in elements]))
            elements = [(int(t), 0) if t.isdigit() else (0, t) for t in elements if pd.notna(t) and t.strip()]
            return page.order, *elements

        self._pages.sort(key=lambda p: order(p))
        return self._pages

    def create_layout(self, layout: PageLayout) -> None:
        layout.container.class_name = "card-container"

        layout.menu = dbc.NavItem(dbc.NavLink(self.name, href=self.path))
        layout.card.add_title(self.name)
        layout.card.add_footer(href=self.path)

        layout.append(dbc.Row(dbc.Col(html.H4(f"{self.name}:"))))

        for page in self.sort():
            if page.layout.has_card_items():
                layout.append(dbc.Row(dbc.Col(page.layout.card, width="auto"), align="stretch"))
