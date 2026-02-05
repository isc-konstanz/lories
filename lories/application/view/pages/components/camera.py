# -*- coding: utf-8 -*-
"""
lories.application.view.pages.components.camera
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import List

import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html

from lories.application.view.pages import ComponentPage, PageLayout, register_component_group, register_component_page
from lories.components.cameras import Camera, CameraProtector


@register_component_page(Camera)
@register_component_group(Camera)
class CameraPage(ComponentPage[Camera]):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(
            order=200,  # Default order is 100. May be lower to be first component shown.
            *args,
            **kwargs,
        )

    @property
    def protection(self) -> CameraProtector:
        return self._component.protection

    def has_protection(self) -> bool:
        return self._component.has_protection()

    def create_layout(self, layout: PageLayout) -> None:
        super().create_layout(layout)

        stream = self._build_stream()
        layout.card.append(stream, focus=True)
        layout.append(dbc.Row(dbc.Col(stream, width="auto")))

        if self.has_protection():
            switch = self._build_switch()
            layout.card.append(switch)
            layout.append(dbc.Row(dbc.Col(switch, width="auto")))

    # noinspection PyShadowingBuiltins
    def _build_switch(self) -> html.Div:
        id = f"{self.id}-protection-state"

        @callback(
            Input(id, "value"),
            force_no_output=True,
        )
        def _update_state(state: bool) -> None:
            _state = self.protection.data.state
            if _state.is_valid() and _state.value != state:
                _state.write(state)

        @callback(
            Output(id, "value"),
            Input(f"{id}-update", "n_intervals"),
        )
        def _update_switch(*_) -> bool:
            _state = self.protection.data.state
            if _state.is_valid():
                return _state.value
            return False

        return html.Div(
            [
                html.H5("State"),
                dbc.Switch(
                    id=id,
                    # label="State",
                    style={"fontSize": "1.5rem"},
                    value=_update_switch(),
                ),
                dcc.Interval(
                    id=f"{id}-update",
                    interval=60000,
                    n_intervals=0,
                ),
            ]
        )

    def _build_stream(self) -> html.Div:
        @callback(
            Output(f"{self.id}-stream", "children"),
            Input(f"{self.id}-stream-update", "n_intervals"),
        )
        def _update_stream(*_) -> List[html.P] | dbc.Spinner:
            return dbc.Spinner(html.Div(id=f"{self.id}-stream-loader"))

        return html.Div(
            [
                html.Div(
                    _update_stream(),
                    id=f"{self.id}-stream",
                ),
                dcc.Interval(
                    id=f"{self.id}-stream-update",
                    interval=60000,
                    n_intervals=0,
                ),
            ]
        )
