# -*- coding: utf-8 -*-
"""
lories.application.view.pages.components.page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import base64
from collections.abc import Sequence
from typing import Collection, Generic, List, Optional

import dash_bootstrap_components as dbc
from dash import Input, Output, callback, html

import pandas as pd
from lories.application.view.pages import Page, PageLayout
from lories.application.view.pages.widgets import build_configs_editor_modal
from lories.typing import Channel, Channels, Component, Components, Configurations, Connector, Connectors, Data


class ComponentPage(Page, Generic[Component]):
    _component: Component

    def __init__(self, component: Component, *args, **kwargs) -> None:
        super().__init__(
            id=component.id,
            key=component.key,
            name=component.name,
            *args,
            **kwargs,
        )
        self._component = component

    @property
    def configs(self) -> Configurations:
        return self._component.configs

    @property
    def connectors(self) -> Connectors:
        return self._component.connectors

    @property
    def components(self) -> Components:
        return self._component.components

    @property
    def data(self) -> Data:
        return self._component.data

    def is_active(self) -> bool:
        return self._component.is_active()

    def create_layout(self, layout: PageLayout) -> None:
        super().create_layout(layout)
        open_button, modal = build_configs_editor_modal(
            entity_id=self.id,
            configs=self.configs,
            configurator_type=type(self._component),
            components=list(self._component.components.values()),
            connectors=list(self._component.connectors.values()),
        )
        layout.card.add_title(self.title)
        layout.card.add_footer(href=self.path)
        layout.append(
            dbc.Row(
                [
                    dbc.Col(html.H4(f"{self.title}:", className="mb-0"), width="auto"),
                    dbc.Col(open_button, width="auto", className="ms-auto"),
                ],
                className="align-items-center mb-0 g-2",
            )
        )
        layout.append(modal)

    def _on_create_layout(self, layout: PageLayout) -> None:
        super()._on_create_layout(layout)
        self._create_data_layout(layout, self.data.channels)
        self._create_components_layout(layout)
        self._create_connectors_layout(layout)

    def _create_data_layout(self, layout: PageLayout, channels: Channels, title: Optional[str] = "Data") -> None:
        layout.append(html.Hr())

        if len(channels) > 0:
            content = self._build_data(channels)
        else:
            content = html.Div(html.I("No data channels.", className="text-muted"))

        section_title = title if title is not None else "Data"
        layout.append(
            dbc.Row(
                dbc.Col(
                    dbc.Accordion(
                        dbc.AccordionItem(
                            title=section_title,
                            children=content,
                            item_id=f"{self.id}-section-data",
                        ),
                        active_item=f"{self.id}-section-data",
                        always_open=True,
                    )
                )
            )
        )

        # TODO: append data-update separately to view

    def _build_data(self, channels: Channels) -> html.Div:
        @callback(
            Output(f"{self.id}-data", "children"),
            Input("view-update", "n_intervals"),
        )
        def _update_data(*_) -> Sequence[dbc.AccordionItem]:
            return [self._build_channel(channel) for channel in channels]

        return html.Div(
            [
                dbc.Accordion(
                    id=f"{self.id}-data",
                    children=_update_data(),
                    start_collapsed=True,
                    always_open=True,
                    flush=True,
                ),
            ]
        )

    def _build_channel(self, channel: Channel) -> dbc.AccordionItem:
        return dbc.AccordionItem(
            title=dbc.Row(
                [
                    dbc.Col(self._build_channel_title(channel), width="auto"),
                    dbc.Col(self._build_channel_header(channel), width="auto"),
                ],
                justify="between",
                className="w-100",
            ),
            children=[
                dbc.Row(
                    [
                        dbc.Col(
                            html.Span("Value:", className="text-muted"),
                            width=1,
                            style={"minWidth": "5.5rem"},
                        ),
                        dbc.Col(
                            [
                                self._build_channel_value(channel),
                                self._build_channel_unit(channel),
                            ],
                            width="auto",
                        ),
                    ],
                    justify="start",
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            html.Span("Updated:", className="text-muted"),
                            width=1,
                            style={"minWidth": "5.5rem"},
                        ),
                        dbc.Col(self._build_channel_timestamp(channel), width="auto"),
                    ],
                    justify="start",
                ),
                dbc.Row(
                    [
                        dbc.Col(None, width=1, style={"minWidth": "5.5rem"}),
                        dbc.Col(self._build_channel_body(channel), width="auto"),
                    ],
                    justify="start",
                ),
            ],
            id=f"{self.id}-data-{self._encode_id(channel.key)}",
        )

    # noinspection PyMethodMayBeStatic
    def _build_channel_title(self, channel: Channel) -> html.Span:
        # TODO: Implement future improvements like the separation of name and unit
        return html.Span(channel.name, className="mb-1")

    # noinspection PyMethodMayBeStatic
    def _build_channel_header(self, channel: Channel) -> Collection[html.Span]:
        channel_header = []
        if channel.is_valid():
            channel_header.append(self._build_channel_value(channel))
            channel_header.append(self._build_channel_unit(channel))
        channel_header.append(self._build_channel_state(channel))
        return channel_header

    # noinspection PyMethodMayBeStatic
    def _build_channel_body(self, channel: Channel) -> Optional[html.Div]:
        if not channel.is_valid():
            return None
        if channel.type == bytes:
            if bool(channel.get("stream", default=False)):
                return html.Div(
                    html.Img(
                        src=f"/api/stream/{channel.id}",
                        style={"maxWidth": "100%", "height": "auto"},
                    )
                )
            value = channel.value
            if value is None or not isinstance(value, (bytes, bytearray)):
                return None
            encoded = base64.b64encode(value).decode("ascii")
            return html.Div(
                html.Img(
                    src=f"data:image/jpeg;base64,{encoded}",
                    style={"maxWidth": "100%", "height": "auto"},
                )
            )
        if not (channel.has_logger() or channel.type == pd.Series):
            return None

        # TODO: Implement a Graph for logged values or pandas.Series types
        return html.Div(html.I("Placeholder", className="text-muted"))

    # noinspection PyMethodMayBeStatic
    def _build_channel_timestamp(self, channel: Channel) -> html.Small:
        timestamp = channel.timestamp
        if not pd.isna(timestamp):
            timestamp = timestamp.isoformat(sep=" ", timespec="seconds")
        return html.Small(timestamp, className="text-muted")

    # noinspection PyMethodMayBeStatic
    def _build_channel_value(self, channel: Channel) -> html.Span:
        # TODO: Implement further type validation
        value = channel.value
        if pd.isna(value):
            return html.Span("—", className="text-muted mb-1", style={"margin-right": "0.2rem"})
        if channel.type == bytes:
            label = "(streaming)" if bool(channel.get("stream", default=False)) else "(image)"
            return html.Span(label, className="text-muted mb-1", style={"margin-right": "0.2rem"})
        if channel.type == float:
            value = round(channel.value, 2)
        # React does not render bare bools (False → empty). Stringify so the
        # channel value is always visible.
        return html.Span(str(value), className="mb-1", style={"margin-right": "0.2rem"})

    # noinspection PyMethodMayBeStatic
    def _build_channel_unit(self, channel: Channel) -> html.Span:
        return html.Span(channel.unit, className="text-muted", style={"margin-right": "2rem"})

    # noinspection PyMethodMayBeStatic
    def _build_channel_state(self, channel: Channel) -> html.Small:
        state = str(channel.state).replace("_", " ")
        color = "success" if channel.is_valid() else "warning"
        if state.lower().endswith("error") or state.lower() == "disabled":
            color = "danger"
        return html.Small(state.title(), className=f"text-{color}", style={"margin-right": "1rem"})

    def _create_connectors_layout(self, layout: PageLayout) -> None:
        connectors = list(self._component.connectors.values())
        if connectors:
            content = self._build_connectors(connectors)
        else:
            content = html.Div(html.I("No connectors.", className="text-muted"))
        layout.append(html.Hr())
        layout.append(
            dbc.Row(
                dbc.Col(
                    dbc.Accordion(
                        dbc.AccordionItem(
                            title="Connectors",
                            children=content,
                            item_id=f"{self.id}-section-connectors",
                        ),
                        active_item=f"{self.id}-section-connectors",
                        always_open=True,
                    )
                )
            )
        )

    def _build_connectors(self, connectors: List[Connector]) -> html.Div:
        @callback(
            Output(f"{self.id}-connectors", "children"),
            Input("view-update", "n_intervals"),
        )
        def _update(*_):
            return [self._build_connector_item(c) for c in connectors]

        return html.Div(
            dbc.Accordion(
                id=f"{self.id}-connectors",
                children=_update(),
                start_collapsed=True,
                always_open=True,
                flush=True,
            )
        )

    def _build_connector_item(self, connector: Connector) -> dbc.AccordionItem:
        href = f"/connector/{self._encode_id(connector.id)}"

        if not connector.is_enabled():
            badge = dbc.Badge("Disabled", color="secondary")
            timestamp_str = "—"
        elif connector._is_connected():
            badge = dbc.Badge("Connected", color="success")
            ts = connector._timestamp_connect
            timestamp_str = ts.isoformat(sep=" ", timespec="seconds") if not pd.isna(ts) else "—"
        else:
            badge = dbc.Badge("Disconnected", color="danger")
            ts = connector._timestamp_disconnect
            timestamp_str = ts.isoformat(sep=" ", timespec="seconds") if not pd.isna(ts) else "—"

        return dbc.AccordionItem(
            title=dbc.Row(
                [
                    dbc.Col(html.A(connector.name, href=href), width="auto"),
                    dbc.Col(
                        [
                            html.Small(
                                type(connector).__name__,
                                className="text-muted",
                                style={"marginRight": "2rem"},
                            ),
                            badge,
                        ],
                        width="auto",
                    ),
                ],
                justify="between",
                className="w-100",
            ),
            children=dbc.Row(
                [
                    dbc.Col(html.Span("Since:", className="text-muted"), width=1),
                    dbc.Col(html.Small(timestamp_str, className="text-muted"), width="auto"),
                ],
                justify="start",
            ),
            id=f"{self.id}-conn-{self._encode_id(connector.key)}",
        )

    def _create_components_layout(self, layout: PageLayout) -> None:
        components = list(self._component.components.values())
        if components:
            content = self._build_components(components)
        else:
            content = html.Div(html.I("No components.", className="text-muted"))
        layout.append(html.Hr())
        layout.append(
            dbc.Row(
                dbc.Col(
                    dbc.Accordion(
                        dbc.AccordionItem(
                            title="Components",
                            children=content,
                            item_id=f"{self.id}-section-components",
                        ),
                        active_item=f"{self.id}-section-components",
                        always_open=True,
                    )
                )
            )
        )

    def _build_components(self, components: List[Component]) -> html.Div:
        @callback(
            Output(f"{self.id}-components", "children"),
            Input("view-update", "n_intervals"),
        )
        def _update(*_):
            return [self._build_component_item(c) for c in components]

        return html.Div(
            dbc.Accordion(
                id=f"{self.id}-components",
                children=_update(),
                start_collapsed=True,
                always_open=True,
                flush=True,
            )
        )

    def _build_component_item(self, component: Component) -> dbc.AccordionItem:
        href = f"{self.path}/{self._encode_id(component.key)}"

        if not component.is_enabled():
            badge = dbc.Badge("Disabled", color="secondary")
        elif component.is_active():
            badge = dbc.Badge("Active", color="success")
        else:
            badge = dbc.Badge("Inactive", color="warning")

        return dbc.AccordionItem(
            title=dbc.Row(
                [
                    dbc.Col(html.A(component.name, href=href), width="auto"),
                    dbc.Col(
                        [
                            html.Small(
                                type(component).__name__,
                                className="text-muted",
                                style={"marginRight": "2rem"},
                            ),
                            badge,
                        ],
                        width="auto",
                    ),
                ],
                justify="between",
                className="w-100",
            ),
            children=dbc.Row(
                [
                    dbc.Col(html.Span("Type:", className="text-muted"), width=1),
                    dbc.Col(html.Small(type(component).__name__, className="text-muted"), width="auto"),
                ],
                justify="start",
            ),
            id=f"{self.id}-comp-{self._encode_id(component.key)}",
        )
