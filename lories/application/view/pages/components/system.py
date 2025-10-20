# -*- coding: utf-8 -*-
"""
lories.application.view.pages.components.system
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories.application.view.pages import PageLayout, register_component_page
from lories.application.view.pages.components import ComponentGroup
from lories.system import System


@register_component_page(System)
class SystemPage(ComponentGroup[System]):
    @property
    def path(self) -> str:
        return f"/system/{self._encode_id(self.key)}"

    def create_layout(self, layout: PageLayout) -> None:
        super().create_layout(layout)
        # TODO: Implement location
