# -*- coding: utf-8 -*-
"""
loris.app.view.pages.layout.page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import MutableSequence
from typing import Collection, Optional, TypeVar

from dash.development.base_component import Component
from dash_bootstrap_components import Container

from loris.app.view.pages.layout import PageCard

C = TypeVar("C", bound=Component)


class PageLayout(MutableSequence[C]):
    menu: Optional[C]
    card: PageCard

    container: Container

    # noinspection PyPep8Naming
    def __init__(
        self,
        children: Collection[C] = (),
        class_name: Optional[str] = None,
        className: Optional[str] = None,
        menu: Optional[C] = None,
        **kwargs,
    ) -> None:
        if class_name is None or len(class_name) == 0:
            class_name = className
        if class_name is None or len(class_name) == 0:
            class_name = "page-container"
        else:
            class_name = f"{class_name} page-container"

        self.container = Container(
            children=list[C](children),
            class_name=class_name,
            **kwargs,
        )
        self.card = PageCard()
        self.menu = menu

    def __len__(self) -> int:
        return len(self.container)

    def __getitem__(self, index: int) -> C:
        return self.container[index]

    def __setitem__(self, index: int, value: C) -> None:
        self.container[index] = value

    def __delitem__(self, index: int) -> None:
        del self.container[index]

    def insert(self, index: int, value: C) -> None:
        self.container.children.insert(index, value)

    def has_card_items(self) -> bool:
        return self.card.has_items()

    def has_menu_item(self) -> bool:
        return self.menu is not None
