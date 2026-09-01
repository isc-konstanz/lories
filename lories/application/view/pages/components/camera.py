# -*- coding: utf-8 -*-
"""
lories.application.view.pages.components.camera
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import annotations

from typing import Optional

import dash_bootstrap_components as dbc
from dash import Input, Output, callback, ctx, dcc, html

import pandas as pd
from lories.application.view.pages import ComponentPage, PageLayout, register_component_group, register_component_page
from lories.components.cameras import Camera, CameraProtector
from lories.components.cameras._core import _Camera
from lories.typing import Channel

_FRAME_BOX_STYLE = {
    "width": "100%",
    "aspectRatio": "16 / 9",
    "overflow": "hidden",
}
_FRAME_IMG_STYLE = {
    "width": "100%",
    "height": "100%",
    "objectFit": "contain",
    "display": "block",
}


def _frame_box(img: html.Img) -> html.Div:
    # Wrapper holds the aspect-ratio so the box reserves space before the
    # <img> has any intrinsic size (an unloaded <img> is 0x0).
    return html.Div(img, style=_FRAME_BOX_STYLE)


@register_component_page(Camera)
@register_component_group(Camera)
class CameraPage(ComponentPage[Camera]):
    _CHANNEL_VIEWS = (
        (_Camera.FRAME, "Frame"),
        (_Camera.STREAM, "Stream"),
        (_Camera.MOTION, "Motion"),
    )

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

        channels = self._build_channels() if self._component.preview else None
        if channels is not None:
            layout.card.append(channels, focus=True)
            # Default width (12), not "auto": an auto Col shrinks to its
            # children's intrinsic size — 0 px while frames haven't loaded.
            layout.append(dbc.Row(dbc.Col(channels)))

        if self.has_protection():
            if channels is not None:
                layout.card.append(html.Hr())
                layout.append(html.Hr())
            switch = self._build_switch()
            layout.card.append(switch)
            layout.append(dbc.Row(dbc.Col(switch)))

    def _build_switch(self) -> html.Div:
        button_id = f"{self.id}-protection-trigger"
        status_id = f"{button_id}-status"

        @callback(
            Output(status_id, "children"),
            Output(status_id, "className"),
            Output(button_id, "disabled"),
            Input(button_id, "n_clicks"),
            Input("view-update", "n_intervals"),
            prevent_initial_call=False,
        )
        def _on_click(n_clicks, _):
            if ctx.triggered_id == button_id and n_clicks:
                self.protection.trigger()
            state = self.protection.data.protection_state
            closed = state.is_valid() and bool(state.value)
            if closed:
                return "Closed — protection running", "fs-5 fw-semibold text-warning", True
            if self.protection.is_in_cooldown():
                return "Cooldown — motion ignored", "fs-5 text-muted", True
            return "Idle — shutter open", "fs-5 text-muted", False

        return html.Div(
            [
                html.H5("Protection"),
                html.Div(
                    [
                        html.Span("Idle — shutter open", id=status_id, className="fs-5 text-muted"),
                        dbc.Button("Start protection", id=button_id, color="success"),
                    ],
                    style={
                        "display": "flex",
                        "justifyContent": "space-between",
                        "alignItems": "center",
                        "gap": "1rem",
                        "width": "100%",
                    },
                ),
            ]
        )

    def _build_channels(self) -> html.Div | None:
        sections = []
        for key, label in self._CHANNEL_VIEWS:
            if key not in self._component.data:
                continue
            channel = self._component.data.get(key)
            viewer = (
                self._build_snapshot() if key == _Camera.FRAME else self._build_mjpeg(channel.id, f"{self.id}-{key}")
            )
            # flex: 1 1 0 + min-width: 0 forces equal column shares; without
            # the min-width override the <img>'s intrinsic size would act as
            # a min-width floor on the flex item.
            sections.append(
                html.Div(
                    [html.H5(label), viewer],
                    style={
                        "flex": "1 1 0",
                        "minWidth": 0,
                        "border": "1px solid var(--bs-border-color, #dee2e6)",
                        "borderRadius": "0.375rem",
                        "padding": "0.75rem",
                    },
                )
            )
        if not sections:
            return None
        return html.Div(
            sections,
            style={"display": "flex", "gap": "1rem", "width": "100%", "alignItems": "flex-start"},
        )

    def _build_snapshot(self) -> html.Div:
        url = f"/api/snapshot/{self._component.id}"
        img_id = f"{self.id}-snapshot"
        interval_id = f"{self.id}-snapshot-update"

        @callback(Output(img_id, "src"), Input(interval_id, "n_intervals"))
        def _refresh(_n_intervals: int) -> str:
            # Cache-bust with the channel's update timestamp: between reads it
            # stays the same, Dash diffs no-op, browser never refetches. Only
            # an actual frame update changes the src and triggers a GET.
            return f"{url}?t={self._frame_cache_key()}"

        return html.Div(
            [
                _frame_box(html.Img(id=img_id, src=url, style=_FRAME_IMG_STYLE)),
                dcc.Interval(id=interval_id, interval=1000, n_intervals=0),
            ]
        )

    def _frame_cache_key(self) -> str:
        frame = self._component.data.get(_Camera.FRAME) if _Camera.FRAME in self._component.data else None
        if frame is None or pd.isna(frame.timestamp):
            return "0"
        return str(int(frame.timestamp.timestamp()))

    def _build_mjpeg(self, channel_id: str, element_id: str) -> html.Div:
        return _frame_box(
            html.Img(id=element_id, src=f"/api/stream/{channel_id}", style=_FRAME_IMG_STYLE),
        )

    def _build_channel_body(self, channel: Channel) -> Optional[html.Div]:
        # For bytes channels owned by this camera, route through the same
        # endpoints the channels block uses — base64 in the accordion would
        # otherwise show a frozen snapshot for non-streaming channels.
        if channel.type == bytes and channel.id in self._component.data:
            if not self._component.preview:
                return html.Div(html.I("Preview disabled", className="text-muted"))
            if bool(channel.get("stream", default=False)):
                src = f"/api/stream/{channel.id}"
            else:
                # Cache-bust with the frame's update timestamp so the browser
                # only refetches when the channel actually updates.
                src = f"/api/snapshot/{self._component.id}?t={self._frame_cache_key()}"
            return html.Div(html.Img(src=src, style={"maxWidth": "100%", "height": "auto"}))
        return super()._build_channel_body(channel)
