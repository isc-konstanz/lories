# -*- coding: utf-8 -*-
"""
lories.application.view.pages.components.camera
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import base64
from typing import Optional

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
                html.H5("Protection"),
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
        has_stream = Camera.STREAM in self.data
        inference = getattr(self._component, "inference", None)
        show_toggle = has_stream and inference is not None and inference.is_enabled()
        predictions_id = f"{self.id}-live-predictions"

        @callback(
            Output(f"{self.id}-stream", "children"),
            Input(f"{self.id}-stream-update", "n_intervals"),
            Input(predictions_id, "value"),
        )
        def _update_stream(_n_intervals: int, show_predictions: bool) -> html.Img | dbc.Spinner:
            frame = self._stream_frame(show_predictions)
            if frame is None:
                return dbc.Spinner(html.Div(id=f"{self.id}-stream-loader"))
            encoded = base64.b64encode(frame).decode("ascii")
            return html.Img(src=f"data:image/jpeg;base64,{encoded}", style={"max-width": "100%", "height": "auto"})

        return html.Div(
            [
                html.Div(
                    _update_stream(0, False),
                    id=f"{self.id}-stream",
                ),
                dcc.Interval(
                    id=f"{self.id}-stream-update",
                    interval=1000,
                    n_intervals=0,
                ),
                dbc.Switch(
                    id=predictions_id,
                    label="Live Predictions",
                    value=False,
                    style={"display": "block" if show_toggle else "none"},
                ),
            ]
        )

    def _stream_frame(self, show_predictions: bool) -> Optional[bytes]:
        if Camera.STREAM not in self.data:
            return None
        stream = self.data.stream
        if not stream.is_valid():
            return None

        if show_predictions:
            inference = getattr(self._component, "inference", None)
            if inference is not None:
                preview = getattr(inference.data, "preview", None)
                if preview is not None and preview.is_valid():
                    return preview.value
        return stream.value
