# -*- coding: utf-8 -*-
"""
loris.app.view.pages.group
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import MutableSequence
from typing import Collection, Generic, Iterator, List, Optional, TypeVar

from loris.app.view.pages import Page

P = TypeVar("P", bound=Page)


# noinspection PyAbstractClass
class PageGroup(Page, MutableSequence[P], Generic[P]):
    _pages: List[P]

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

    @property
    def path(self) -> str:
        return f"/{self._encode_id(self.name.lower())}"
