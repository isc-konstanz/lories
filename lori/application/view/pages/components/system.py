# -*- coding: utf-8 -*-
"""
lori.application.view.pages.components.system
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori import System
from lori.application.view.pages import PageLayout, register_component_page
from lori.application.view.pages.components import ComponentGroup


@register_component_page(System)
class SystemPage(ComponentGroup[System]):
    @property
    def path(self) -> str:
        return f"/system/{self._encode_id(self.key)}"

    def create_layout(self, layout: PageLayout) -> None:
        super().create_layout(layout)
        # TODO: Implement location
