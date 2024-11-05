# -*- coding: utf-8 -*-
"""
lori.app.view.pages.components.page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Generic, Optional, TypeVar

import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html

import pandas as pd
from lori import Channel, Component, Configurations
from lori.app.view.pages import Page, PageLayout
from lori.connectors import ConnectorAccess
from lori.data import DataAccess

C = TypeVar("C", bound=Component)


class ComponentPage(Page, Generic[C]):
    _component: C

    def __init__(self, component: C, *args, **kwargs) -> None:
        super().__init__(
            id=component.id,
            name=component.name,
            *args,
            **kwargs,
        )
        self._component = component

    @property
    def configs(self) -> Configurations:
        return self._component.configs

    @property
    def connectors(self) -> ConnectorAccess:
        return self._component.connectors

    @property
    def data(self) -> DataAccess:
        return self._component.data

    @property
    def path(self) -> str:
        if isinstance(self._component.context, Component):
            return f"/{self._encode_id(self._component.context.key)}/{self._encode_id(self._component.key)}"
        else:
            return f"/{self._encode_id(self._component.key)}"

    def is_active(self) -> bool:
        return self._component.is_active()

    def create_layout(self, layout: PageLayout) -> None:
        layout.card.add_title(self.name)
        layout.card.add_footer(href=self.path)
        layout.append(html.H4(f"{self.name}:"))

    def _on_create_layout(self, layout: PageLayout) -> None:
        super()._on_create_layout(layout)
        self._create_data_layout(layout)

    def _create_data_layout(self, layout: PageLayout, title: Optional[str] = "Data") -> None:
        if len(self.data.channels) > 0:
            layout.append(html.Hr())

            data = []
            if title is not None:
                data.append(html.H5(f"{title}:"))
            data.append(self._build_data())

            layout.append(dbc.Row(data))

    def _build_data(self) -> html.Div:
        @callback(
            Output(f"{self.id}-data", "children"),
            Input(f"{self.id}-data-update", "n_intervals"),
        )
        def _update_data(*_) -> Sequence[dbc.AccordionItem]:
            return [self._build_channel(channel) for channel in self.data.channels]

        return html.Div(
            [
                dbc.Accordion(
                    id=f"{self.id}-data",
                    children=_update_data(),
                    start_collapsed=True,
                    always_open=True,
                    flush=True,
                ),
                dcc.Interval(
                    id=f"{self.id}-data-update",
                    interval=1000,
                    n_intervals=0,
                ),
            ]
        )

    def _build_channel(self, channel: Channel) -> dbc.AccordionItem:
        return dbc.AccordionItem(
            title=dbc.Row(
                [
                    dbc.Col(self._build_channel_title(channel), width="auto"),
                    dbc.Col(self._build_channel_state(channel), width="auto"),
                ],
                justify="between",
                className="w-100",
            ),
            children=[
                dbc.Row(
                    [
                        dbc.Col(html.Span("Value:", className="text-muted"), width=1),
                        dbc.Col(self._build_channel_value(channel), width="auto"),
                    ],
                    justify="start",
                ),
                dbc.Row(
                    [
                        dbc.Col(None, width=1),
                        dbc.Col(self._build_channel_timestamp(channel), width="auto"),
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
    def _build_channel_value(self, channel: Channel) -> html.Span:
        # TODO: Implement further type validation, e.g. implementing a Graph for pandas Series types
        value = channel.value
        if not pd.isna(value):
            if channel.type == float:
                value = round(channel.value, 2)
        return html.Span(html.B(value), className="mb-1")

    # noinspection PyMethodMayBeStatic
    def _build_channel_timestamp(self, channel: Channel) -> html.Small:
        timestamp = channel.timestamp
        if not pd.isna(timestamp):
            timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S%:z")
        return html.Small(timestamp, className="text-muted")

    # noinspection PyMethodMayBeStatic
    def _build_channel_state(self, channel: Channel) -> html.Small:
        state = str(channel.state).replace("_", " ")
        color = "success" if channel.is_valid() else "warning"
        if state.lower().endswith("error") or state.lower() == "disabled":
            color = "danger"
        return html.Small(state.title(), className=f"text-{color}", style={"margin-right": "1rem"})
