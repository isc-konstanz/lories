# -*- coding: utf-8 -*-
"""
lori.app.view.pages.layout.page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import MutableSequence
from typing import Any, Collection, Optional

from dash_bootstrap_components import Container

from lori.app.view.pages.layout import PageCard


class PageLayout(MutableSequence[Any]):
    menu: Optional[Any]
    card: PageCard

    container: Container

    # noinspection PyPep8Naming
    def __init__(
        self,
        children: Collection[Any] = (),
        class_name: Optional[str] = None,
        className: Optional[str] = None,
        menu: Optional[Any] = None,
        **kwargs,
    ) -> None:
        if class_name is None or len(class_name) == 0:
            class_name = className
        if class_name is None or len(class_name) == 0:
            class_name = "page-container"
        else:
            class_name = f"{class_name} page-container"

        self.container = Container(
            children=list(children),
            class_name=class_name,
            **kwargs,
        )
        self.card = PageCard()
        self.menu = menu

    def __len__(self) -> int:
        return len(self.container)

    def __getitem__(self, index: int) -> Any:
        return self.container[index]

    def __setitem__(self, index: int, value: Any) -> None:
        self.container[index] = value

    def __delitem__(self, index: int) -> None:
        del self.container[index]

    def insert(self, index: int, value: Any) -> None:
        self.container.children.insert(index, value)

    def has_card_items(self) -> bool:
        return self.card.has_items()

    def has_menu_item(self) -> bool:
        return self.menu is not None
