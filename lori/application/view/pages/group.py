# -*- coding: utf-8 -*-
"""
lori.application.view.pages.group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import re
from collections.abc import MutableSequence, Sequence
from itertools import chain
from typing import Any, Generic, Iterator, List, Optional, Tuple, TypeVar

import dash_bootstrap_components as dbc
from dash import html

import pandas as pd
from lori.application.view.pages import Page, PageLayout

P = TypeVar("P", bound=Page)


class PageGroup(Page, MutableSequence[P], Generic[P]):
    _pages: List[P]
    _key: str

    order: int = 1000

    # noinspection PyShadowingBuiltins
    def __init__(self, key: Optional[str] = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if key is None:
            key = self._encode_id(self.name)
        self._key = key
        self._pages = []

    def __len__(self) -> int:
        return len(self._pages)

    def __iter__(self) -> Iterator[P]:
        return iter(self._pages)

    def __contains__(self, page: P) -> bool:
        return page in self._pages

    def __getitem__(self, index: int) -> P:
        return self._pages[index]

    def __setitem__(self, index: int, page: P) -> None:
        self._pages[index] = page

    def __delitem__(self, index: int) -> None:
        del self._pages[index]

    def insert(self, index: int, page: P) -> None:
        self._pages.insert(index, page)

    @property
    def key(self) -> str:
        return self._key

    def sort(self) -> Sequence[Page]:
        def order(page: Page) -> Tuple[Any, ...]:
            elements = re.split(r"[^0-9A-Za-zäöüÄÖÜß]+", page.id)
            elements = list(chain(*[re.split(r"([0-9])+", t) for t in elements]))
            elements = [int(t) if t.isdigit() else t for t in elements if pd.notna(t) and t.strip()]
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
