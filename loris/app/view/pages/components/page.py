# -*- coding: utf-8 -*-
"""
loris.app.view.pages.components.page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Collection, Generic, Optional, TypeVar

import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html

import pandas as pd
from loris import Channel, Component, Configurations
from loris.app.view.pages import Page, PageLayout
from loris.data import DataAccess

C = TypeVar("C", bound=Component)


class ComponentPage(Page, Generic[C]):
    _component: C

    def __init__(self, component: C, *args, **kwargs) -> None:
        super().__init__(
            id=component.id,
            name=component.name,
            *args, **kwargs
        )
        self._component = component

    # @property
    # def component(self) -> C:
    #     return self._component

    @property
    def data(self) -> DataAccess:
        return self._component.data

    @property
    def configs(self) -> Configurations:
        return self._component.configs

    @property
    def path(self) -> str:
        if isinstance(self._component.context, Component):
            return (f"/{self._encode_id(self._component.context.key)}"
                    f"/{self._encode_id(self._component.key)}")
        else:
            return f"/{self._encode_id(self._component.key)}"

    def is_active(self) -> bool:
        return self._component.is_active()

    def create_layout(self, layout: PageLayout) -> None:
        super().create_layout(layout)
        layout.card = html.Div(
            [
                html.H4(self.name),
                dbc.Alert(f"This is a placeholder for the {self.name} card view", color="secondary"),
            ]
        )

        layout.append(html.H4(f"{self.name}:"))
        layout.append(html.Hr())
        layout.append(dbc.Alert(f"This is a placeholder for the {self.name} component view", color="secondary"))

    def _on_create_layout(self, layout: PageLayout) -> None:
        super()._on_create_layout(layout)
        self._create_data_layout(layout)

    def _create_data_layout(self, layout: PageLayout, title: Optional[str] = "Data") -> None:

        @callback(Output(f"{self.id}-data", "children"),
                  Input(f"{self.id}-data-update-interval", "n_intervals"))
        def _update_data(n_intervals: int):
            return self._get_data()

        children = [html.Hr()]
        if title is not None:
            children.append(html.H5(f"{title}:"))

        children.append(
            dbc.Accordion(
                id=f"{self.id}-data",
                children=self._get_data(),
                start_collapsed=True,
                always_open=True,
                flush=True,
            )
        )
        children.append(
            dcc.Interval(
                id=f"{self.id}-data-update-interval",
                interval=1000,
                n_intervals=0,
            )
        )
        layout.append(html.Div(children))

    def _get_data(self) -> Collection[dbc.AccordionItem]:
        channel_group = []
        for channel in self.data.channels:
            channel_group.append(self._parse_channel(channel))
        return channel_group

    def _parse_channel(self, channel: Channel) -> dbc.AccordionItem:
        channel_id = self._encode_id(channel.id)
        return dbc.AccordionItem(
            title=dbc.Row(
                [
                    dbc.Col(self._parse_channel_title(channel), width="auto"),
                    dbc.Col(self._parse_channel_state(channel), width="auto"),
                ],
                justify="between",
                className="w-100",
            ),
            children=[
                dbc.Row(
                    [
                        dbc.Col(html.Span("Value:", className="text-muted"), width=1),
                        dbc.Col(self._parse_channel_value(channel), width="auto"),
                    ],
                    justify="start",
                ),
                dbc.Row(
                    [
                        dbc.Col(None, width=1),
                        dbc.Col(self._parse_channel_timestamp(channel), width="auto"),
                    ],
                    justify="start",
                ),
            ],
            id=f"{self.id}-data-{channel_id}",
        )

    # noinspection PyMethodMayBeStatic
    def _parse_channel_title(self, channel: Channel) -> html.Span:
        # TODO: Implement future improvements like the separation of name and unit
        return html.Span(channel.name, className="mb-1")

    # noinspection PyMethodMayBeStatic
    def _parse_channel_value(self, channel: Channel) -> html.Span:
        # TODO: Implement further type validation, e.g. implementing a Graph for pandas Series types
        value = channel.value
        if not pd.isna(value):
            if channel.type == float:
                value = round(channel.value, 2)
        return html.Span(html.B(value), className="mb-1")

    # noinspection PyMethodMayBeStatic
    def _parse_channel_timestamp(self, channel: Channel) -> html.Small:
        return html.Small(channel.timestamp, className="text-muted")

    # noinspection PyMethodMayBeStatic
    def _parse_channel_state(self, channel: Channel) -> html.Small:
        state = str(channel.state).replace("_", " ")
        color = "success" if channel.is_valid() else "warning"
        if state.lower().endswith("error") or state.lower() == "disabled":
            color = "danger"
        return html.Small(state.title(), className=f"text-{color}", style={"margin-right": "1rem"})
