# -*- coding: utf-8 -*-
"""
lories.application.view.pages.connectors.page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Generic, TypeVar

import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html, no_update

import pandas as pd
from lories.application.view._dash_format import (
    HEADER_STATE_STYLE,
    HEADER_UNIT_STYLE,
    HEADER_VALUE_STYLE,
    changed_channels,
    format_bytes_label,
    format_number,
)
from lories.application.view.pages.layout import PageLayout
from lories.application.view.pages.page import Page, gate_updates, update_items
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
            connected = self._connector._connected
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
        channels = list(channels)
        fingerprints_id = f"{self.id}-channels-fingerprints"
        requested_id = f"{self.id}-channels-requested"
        received_id = f"{self.id}-channels-received"
        gate_updates(requested_id, received_id)

        item_ids = [self._item_id(channel) for channel in channels]

        @callback(
            Output(f"{self.id}-channels", "children"),
            Output(fingerprints_id, "data"),
            Output(received_id, "data"),
            Input(requested_id, "data"),
            State(fingerprints_id, "data"),
        )
        def _update_channels(requested, shown):
            fingerprints, changed = changed_channels(channels, shown)
            if not changed:
                return no_update, no_update, requested
            updates = {
                index: {
                    "title": self._build_channel_summary(channels[index]),
                    "children": self._build_channel_updated(channels[index]),
                }
                for index in changed
            }
            return update_items(item_ids, updates, redraw=shown is None), fingerprints, requested

        return html.Div(
            [
                dcc.Store(id=fingerprints_id),
                dcc.Store(id=requested_id),
                dcc.Store(id=received_id),
                dbc.Accordion(
                    id=f"{self.id}-channels",
                    children=[self._build_channel(channel) for channel in channels],
                    start_collapsed=True,
                    always_open=True,
                    flush=True,
                ),
            ]
        )

    def _item_id(self, channel: Channel) -> str:
        return f"{self.id}-ch-{self._encode_id(channel.id)}"

    def _build_channel(self, channel: Channel) -> dbc.AccordionItem:
        return dbc.AccordionItem(
            title=self._build_channel_summary(channel),
            children=self._build_channel_updated(channel),
            id=self._item_id(channel),
        )

    # noinspection PyMethodMayBeStatic
    def _build_channel_summary(self, channel: Channel) -> dbc.Row:
        state = str(channel.state).replace("_", " ")
        color = "success" if channel.is_valid() else "warning"
        if state.lower().endswith("error") or state.lower() == "disabled":
            color = "danger"

        value_span = unit_span = None
        if channel.is_valid():
            value = channel.value
            if channel.type == bytes:
                value = format_bytes_label(channel, value)
            elif channel.type == list:
                value = "—" if value is None else f"({len(value)} values)"
            elif not pd.isna(value) and channel.type == float:
                # Two decimals in the plain range ("0.00", "1234.00"), three
                # significant figures below one, scientific notation outside.
                value = format_number(value)
            value_span = html.Span(str(value), className="mb-1")
            unit_span = html.Span(channel.unit, className="text-muted", style={"marginLeft": "0.3rem"})
        # Value | unit | state as fixed-width columns so rows line up and the
        # value/unit boundary is unambiguous; invalid channels keep empty cells.
        header_items = html.Div(
            [
                html.Div(value_span, style=HEADER_VALUE_STYLE),
                html.Div(unit_span, style=HEADER_UNIT_STYLE),
                html.Div(html.Small(state.title(), className=f"text-{color}"), style=HEADER_STATE_STYLE),
            ],
            className="d-flex align-items-baseline",
        )
        return dbc.Row(
            [
                dbc.Col(html.Span(channel.name, className="mb-1"), width="auto"),
                dbc.Col(header_items, width="auto"),
            ],
            justify="between",
            className="w-100",
        )

    # noinspection PyMethodMayBeStatic
    def _build_channel_updated(self, channel: Channel) -> dbc.Row:
        timestamp = channel.timestamp
        timestamp_str = timestamp.isoformat(sep=" ", timespec="seconds") if not pd.isna(timestamp) else "—"
        return dbc.Row(
            [
                dbc.Col(
                    html.Span("Updated:", className="text-muted"),
                    width=1,
                    style={"minWidth": "5.5rem"},
                ),
                dbc.Col(html.Small(timestamp_str, className="text-muted"), width="auto"),
            ],
            justify="start",
        )
