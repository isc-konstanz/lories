# -*- coding: utf-8 -*-
"""
lori.app.view.pages.group
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import re
from collections.abc import MutableSequence, Sequence
from itertools import chain
from typing import Any, Collection, Generic, Iterator, List, Optional, Tuple, TypeVar

import pandas as pd
from lori.app.view.pages import Page

P = TypeVar("P", bound=Page)


# noinspection PyAbstractClass
class PageGroup(Page, MutableSequence[P], Generic[P]):
    _pages: List[P]

    order: int = 1000

    # noinspection PyShadowingBuiltins
    def __init__(self, pages: Optional[Collection[P]] = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if pages is None:
            pages = []
        self._pages = list[P](pages)

    def __len__(self) -> int:
        return len(self._pages)

    def __iter__(self) -> Iterator[P]:
        return iter(self._pages)

    def __contains__(self, page: P) -> bool:
        return page in self._pages

    def __getitem__(self, index: int) -> P:
        return self._pages[index]

    def __setitem__(self, index: int, value: P) -> None:
        self._pages[index] = value

    def __delitem__(self, index) -> None:
        self._pages.remove(index)

    def insert(self, index, value) -> None:
        self._pages.insert(index, value)

    def sort(self) -> Sequence[Page]:
        def order(page: Page) -> Tuple[Any, ...]:
            elements = re.split(r"[^0-9A-Za-zäöüÄÖÜß]+", page.id)
            elements = list(chain(*[re.split(r"([0-9])+", t) for t in elements]))
            elements = [int(t) if t.isdigit() else t for t in elements if pd.notna(t) and t.strip()]
            return page.order, *elements

        self._pages.sort(key=lambda p: order(p))
        return self._pages

    @property
    def path(self) -> str:
        return f"/{self._encode_id(self.name.lower())}"
