# -*- coding: utf-8 -*-
"""
lories.application.view.pages.connectors.page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Generic, TypeVar

import dash_bootstrap_components as dbc
from dash import Input, Output, callback, html

import pandas as pd
from lories.application.view._dash_format import format_bytes_label
from lories.application.view.pages.layout import PageLayout
from lories.application.view.pages.page import Page
from lories.application.view.pages.widgets import build_configs_editor_modal
from lories.connectors import Connector
from lories.typing import Channel, Channels, Configurations

ConnectorType = TypeVar("ConnectorType", bound=Connector)


class ConnectorPage(Page, Generic[ConnectorType]):
    _connector: ConnectorType

    def __init__(self, connector: ConnectorType, *args, **kwargs) -> None:
        super().__init__(
            id=connector.id,
            key=connector.key,
            name=connector.name,
            *args,
            **kwargs,
        )
        self._connector = connector

    @property
    def configs(self) -> Configurations:
        return self._connector.configs

    @property
    def path(self) -> str:
        return f"/connector/{self._encode_id(self.id)}"

    def create_layout(self, layout: PageLayout) -> None:
        open_button, modal = build_configs_editor_modal(
            entity_id=self.id,
            configs=self.configs,
            configurator_type=type(self._connector),
        )
        layout.card.add_header(type(self._connector).__name__)
        layout.card.add_title(self.title)
        layout.card.add_footer(href=self.path)

        layout.append(
            dbc.Row(
                [
                    dbc.Col(html.H4(f"{self.title}:"), width="auto"),
                    dbc.Col(open_button, width="auto", className="ms-auto"),
                ],
                className="align-items-center mb-0",
            )
        )
        layout.append(html.Small(type(self._connector).__name__, className="text-muted"))
        layout.append(modal)
        layout.append(html.Hr())
        layout.append(self._build_status())

        channels = self._connector.channels
        if len(channels) > 0:
            layout.append(html.Hr())
            layout.append(html.H5("Channels:"))
            layout.append(self._build_channels(channels))

    def _build_status(self) -> html.Div:
        @callback(
            Output(f"{self.id}-status", "children"),
            Input("view-update", "n_intervals"),
        )
        def _update_status(*_):
            if not self._connector.is_enabled():
                return [dbc.Badge("Disabled", color="secondary")]
            connected = self._connector._is_connected()
            color = "success" if connected else "danger"
            label = "Connected" if connected else "Disconnected"
            timestamp = self._connector._timestamp_connect if connected else self._connector._timestamp_disconnect
            timestamp_str = timestamp.isoformat(sep=" ", timespec="seconds") if not pd.isna(timestamp) else "—"
            return [
                dbc.Badge(label, color=color, className="me-2"),
                html.Small(timestamp_str, className="text-muted"),
            ]

        return html.Div(id=f"{self.id}-status", children=_update_status())

    def _build_channels(self, channels: Channels) -> html.Div:
        @callback(
            Output(f"{self.id}-channels", "children"),
            Input("view-update", "n_intervals"),
        )
        def _update_channels(*_):
            return [self._build_channel(ch) for ch in channels]

        return html.Div(
            dbc.Accordion(
                id=f"{self.id}-channels",
                children=_update_channels(),
                start_collapsed=True,
                always_open=True,
                flush=True,
            )
        )

    def _build_channel(self, channel: Channel) -> dbc.AccordionItem:
        state = str(channel.state).replace("_", " ")
        color = "success" if channel.is_valid() else "warning"
        if state.lower().endswith("error") or state.lower() == "disabled":
            color = "danger"

        header_items = []
        if channel.is_valid():
            value = channel.value
            if channel.type == bytes:
                value = format_bytes_label(channel, value)
            elif channel.type == list:
                value = "—" if value is None else f"({len(value)} values)"
            elif not pd.isna(value) and channel.type == float:
                # Format with 3 significant figures, trailing zeros preserved
                # (``#`` flag). Reads as "0.00", "0.120", "0.0100" — consistent
                # precision regardless of magnitude. Avoids the prior
                # ``round(value, 2)`` which collapsed sub-0.01 values to 0.0.
                value = f"{value:#.3g}"
            header_items.append(html.Span(str(value), className="mb-1", style={"margin-right": "0.2rem"}))
            header_items.append(html.Span(channel.unit, className="text-muted", style={"margin-right": "2rem"}))
        header_items.append(html.Small(state.title(), className=f"text-{color}", style={"margin-right": "1rem"}))

        timestamp = channel.timestamp
        timestamp_str = timestamp.isoformat(sep=" ", timespec="seconds") if not pd.isna(timestamp) else "—"

        return dbc.AccordionItem(
            title=dbc.Row(
                [
                    dbc.Col(html.Span(channel.name, className="mb-1"), width="auto"),
                    dbc.Col(header_items, width="auto"),
                ],
                justify="between",
                className="w-100",
            ),
            children=dbc.Row(
                [
                    dbc.Col(
                        html.Span("Updated:", className="text-muted"),
                        width=1,
                        style={"minWidth": "5.5rem"},
                    ),
                    dbc.Col(html.Small(timestamp_str, className="text-muted"), width="auto"),
                ],
                justify="start",
            ),
            id=f"{self.id}-ch-{self._encode_id(channel.key)}",
        )
