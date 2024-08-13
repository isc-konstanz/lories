# -*- coding: utf-8 -*-
"""
loris.app.view.pages.components.system
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from dash import html

from loris import System
from loris.app.view.pages import ComponentGroup, ComponentPage, PageLayout


class SystemPage(ComponentGroup, ComponentPage[System]):
    def __init__(self, system: System, *args, **kwargs) -> None:
        super().__init__(component=system, *args, **kwargs)

    @property
    def path(self) -> str:
        return f"/{self._component.TYPE}/{self._encode_id(self._component.id)}"

    def _create_content_layout(self) -> html.Div:
        div = super()._create_content_layout()
        div.children.append(self._create_data_layout())
        return div

    def _do_create_layout(self) -> PageLayout:
        for page in self:
            page._do_create_layout()
        return super()._do_create_layout()

    def _do_register(self) -> None:
        super()._do_register()
        for page in self:
            page._do_register()
