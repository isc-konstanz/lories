# -*- coding: utf-8 -*-
"""
lories.application.view.pages.components.page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import base64
from typing import Collection, Generic, List, Optional

import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback, dcc, html, no_update

import pandas as pd
from lories.application.view._dash_format import (
    HEADER_STATE_STYLE,
    HEADER_UNIT_STYLE,
    HEADER_VALUE_STYLE,
    IMAGE_UNITS,
    changed_channels,
    channel_fingerprint,
    format_bytes_label,
    format_number,
)
from lories.application.view.pages import Page, PageLayout
from lories.application.view.pages.page import gate_updates, update_items
from lories.application.view.pages.widgets import build_configs_editor_modal
from lories.typing import Channel, Channels, Component, Components, Configurations, Connector, Connectors, Data


def _component_fingerprint(component: Component) -> List:
    return [component.is_enabled(), component.is_active()]


def _connector_fingerprint(connector: Connector) -> List:
    def _timestamp(timestamp) -> Optional[str]:
        return None if pd.isna(timestamp) else str(timestamp)

    return [
        connector.is_enabled(),
        connector._connected,
        _timestamp(connector._timestamp_connect),
        _timestamp(connector._timestamp_disconnect),
    ]


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
        channels = list(channels)
        indices = {channel.id: index for index, channel in enumerate(channels)}
        fingerprints_id = f"{self.id}-data-fingerprints"
        requested_id = f"{self.id}-data-requested"
        received_id = f"{self.id}-data-received"
        gate_updates(requested_id, received_id, Input(f"{self.id}-data", "active_item"))

        item_ids = [self._item_id(channel) for channel in channels]

        @callback(
            Output(f"{self.id}-data", "children"),
            Output(fingerprints_id, "data"),
            Output(received_id, "data"),
            Input(requested_id, "data"),
            State(f"{self.id}-data", "active_item"),
            State(fingerprints_id, "data"),
        )
        def _update_data(requested, active, shown):
            fingerprints, changed = changed_channels(channels, (shown or {}).get("channels"))
            details = dict((shown or {}).get("details", {}))
            opened = [indices[i] for i in active or [] if i in indices]
            stale = [index for index in opened if details.get(channels[index].id) != fingerprints[index]]
            if not changed and not stale:
                return no_update, no_update, requested

            updates = {index: {"title": self._build_channel_summary(channels[index])} for index in changed}
            for index in stale:
                updates.setdefault(index, {})["children"] = self._build_channel_details(channels[index])
                details[channels[index].id] = fingerprints[index]
            items = update_items(item_ids, updates, redraw=shown is None)
            return items, {"channels": fingerprints, "details": details}, requested

        return html.Div(
            [
                dcc.Store(id=fingerprints_id),
                dcc.Store(id=requested_id),
                dcc.Store(id=received_id),
                dbc.Accordion(
                    id=f"{self.id}-data",
                    children=[self._build_channel(channel) for channel in channels],
                    start_collapsed=True,
                    always_open=True,
                    flush=True,
                ),
            ]
        )

    def _item_id(self, channel: Channel) -> str:
        return f"{self.id}-data-{self._encode_id(channel.key)}"

    def _build_channel(self, channel: Channel) -> dbc.AccordionItem:
        return dbc.AccordionItem(
            title=self._build_channel_summary(channel),
            id=self._item_id(channel),
            item_id=channel.id,
        )

    def _build_channel_summary(self, channel: Channel) -> dbc.Row:
        return dbc.Row(
            [
                dbc.Col(self._build_channel_title(channel), width="auto"),
                dbc.Col(self._build_channel_header(channel), width="auto"),
            ],
            justify="between",
            className="w-100",
        )

    def _build_channel_details(self, channel: Channel) -> List[dbc.Row]:
        return [
            self._build_channel_row("ID:", html.Small(channel.id, className="text-muted font-monospace")),
            self._build_channel_row("Type:", html.Small(channel.type.__name__, className="text-muted")),
            self._build_channel_row("Interval:", html.Small(channel.freq or "—", className="text-muted")),
            self._build_channel_row("Connector:", self._build_channel_member(channel.connector)),
            self._build_channel_row("Logger:", self._build_channel_member(channel.logger)),
            self._build_channel_row(
                "Value:",
                [
                    self._build_channel_value(channel),
                    self._build_channel_unit(channel),
                ],
            ),
            self._build_channel_row("Updated:", self._build_channel_timestamp(channel)),
            self._build_channel_row(None, self._build_channel_body(channel)),
        ]

    # noinspection PyMethodMayBeStatic
    def _build_channel_row(self, label: Optional[str], content) -> dbc.Row:
        return dbc.Row(
            [
                dbc.Col(
                    html.Span(label, className="text-muted") if label else None,
                    width=1,
                    style={"minWidth": "6.5rem"},
                ),
                dbc.Col(content, width="auto"),
            ],
            justify="start",
        )

    # noinspection PyMethodMayBeStatic
    def _build_channel_member(self, member) -> html.Small:
        if member is None or not member.enabled or member.id is None:
            return html.Small("—", className="text-muted")
        return html.Small(member.id, className="text-muted font-monospace")

    # noinspection PyMethodMayBeStatic
    def _build_channel_title(self, channel: Channel) -> html.Span:
        # TODO: Implement future improvements like the separation of name and unit
        return html.Span(channel.name, className="mb-1")

    # noinspection PyMethodMayBeStatic
    def _build_channel_header(self, channel: Channel) -> html.Div:
        """Value | unit | state as three fixed-width columns. Invalid channels
        keep empty value and unit cells so the state column still lines up."""
        valid = channel.is_valid()
        return html.Div(
            [
                html.Div(self._build_channel_value(channel) if valid else None, style=HEADER_VALUE_STYLE),
                html.Div(self._build_channel_unit(channel) if valid else None, style=HEADER_UNIT_STYLE),
                html.Div(self._build_channel_state(channel), style=HEADER_STATE_STYLE),
            ],
            className="d-flex align-items-baseline",
        )

    # noinspection PyMethodMayBeStatic
    def _build_channel_body(self, channel: Channel) -> Optional[html.Div]:
        if not channel.is_valid():
            return None
        if channel.type == bytes:
            if bool(channel.get("stream", default=False)):
                return html.Div(
                    html.Img(
                        src=f"/api/image/{channel.id}?v={channel_fingerprint(channel)}",
                        style={"maxWidth": "100%", "height": "auto"},
                    )
                )
            value = channel.value
            # Multi-row channel values (e.g. the soil predictor publishes a
            # length-N pd.Series of PNG bytes — one frame per save tick)
            # go through the same timestamp/value table as float Series.
            # The table renderer detects bytes values and emits ``<img>``
            # cells when the channel unit is an image format, or a size
            # summary otherwise.
            if isinstance(value, pd.Series):
                return self._build_channel_series_table(channel, value)
            return self._build_bytes_img(channel)
        if channel.type == list:
            value = channel.value
            if value is None:
                return html.Span("—", className="text-muted")
            return self._build_channel_list_table(channel, value)
        # Multi-row Series values (predictor trajectories, timestamp_creation
        # series, etc.) render as a two-column timestamp/value table so the
        # full horizon is readable when the channel accordion is expanded.
        value = channel.value
        if isinstance(value, pd.Series):
            return self._build_channel_series_table(channel, value)
        if not (channel.has_logger() or channel.type == pd.Series):
            return None

        # TODO: Implement a Graph for logged values or pandas.Series types
        return html.Div(html.I("Placeholder", className="text-muted"))

    # noinspection PyMethodMayBeStatic
    def _build_bytes_img(self, channel: Channel) -> Optional[html.Div]:
        """``<img>`` from ``/api/image`` for image units, ``None`` otherwise."""
        value = channel.value
        unit = (channel.unit or "").strip().lower()
        if unit not in IMAGE_UNITS or not isinstance(value, (bytes, bytearray)) or len(value) == 0:
            return None
        return html.Div(
            html.Img(
                src=f"/api/image/{channel.id}?v={channel_fingerprint(channel)}",
                style={"maxWidth": "100%", "height": "auto"},
            )
        )

    # noinspection PyMethodMayBeStatic
    def _build_channel_series_table(self, channel: Channel, series: pd.Series) -> dbc.Table:
        """Two-column timestamp / value table for a multi-row channel value.
        Float values render with 3-significant-figure precision (same as
        the accordion header); timestamps as ``YYYY-MM-DD HH:MM:SS``.
        Image-unit bytes are rendered as inline ``<img>`` cells; other
        bytes get a length summary so opaque blobs don't print as garbage."""
        unit = (channel.unit or "").strip()
        unit_is_image = unit.lower() in IMAGE_UNITS

        def _fmt_value(v):
            """Returns either a string (for text cells) or a Dash component
            (for image cells). Callers wrap text returns in a Td with the
            channel unit; component returns go straight into the Td."""
            if v is None:
                return "—"
            if isinstance(v, (bytes, bytearray, memoryview)):
                if unit_is_image and len(v) > 0:
                    encoded = base64.b64encode(bytes(v)).decode("ascii")
                    return html.Img(
                        src=f"data:image/jpeg;base64,{encoded}",
                        style={"maxWidth": "100%", "height": "auto"},
                    )
                return f"({len(v):,} bytes)"
            if isinstance(v, float):
                # NaN turns into a clean em-dash so missing-by-design rows
                # (e.g. predictor diagnostics at the IC row) don't print "nan".
                if pd.isna(v):
                    return "—"
                return format_number(v)
            if isinstance(v, pd.Timestamp):
                return v.isoformat(sep=" ", timespec="seconds")
            return str(v)

        def _fmt_index(idx):
            if isinstance(idx, pd.Timestamp):
                return idx.isoformat(sep=" ", timespec="seconds")
            return str(idx)

        idx_style = {
            "padding": "0.15rem 0.5rem",
            "color": "var(--bs-secondary-color, #6c757d)",
            "whiteSpace": "nowrap",
            "verticalAlign": "top",
        }
        val_style_text = {
            "padding": "0.15rem 0.5rem",
            "textAlign": "right",
            "fontVariantNumeric": "tabular-nums",
        }
        val_style_image = {
            "padding": "0.25rem 0.5rem",
        }

        def _value_td(v):
            formatted = _fmt_value(v)
            if isinstance(formatted, str):
                label = f"{formatted} {unit}".rstrip() if unit and not unit_is_image else formatted
                return html.Td(label, style=val_style_text)
            return html.Td(formatted, style=val_style_image)

        rows = [
            html.Tr(
                [
                    html.Td(_fmt_index(idx), style=idx_style),
                    _value_td(v),
                ]
            )
            for idx, v in series.items()
        ]
        th_base = {"padding": "0.15rem 0.5rem", "fontWeight": "normal"}
        header = html.Thead(
            html.Tr(
                [
                    html.Th("Timestamp", style=th_base),
                    html.Th("Value", style={**th_base, "textAlign": "right"}),
                ]
            )
        )
        return dbc.Table(
            [header, html.Tbody(rows)],
            size="sm",
            striped=True,
            bordered=False,
            hover=True,
            className="mb-1",
            style={
                "minWidth": "20rem",
                "width": "auto",
                "border": "1px solid var(--bs-border-color, #dee2e6)",
                "borderRadius": "0.25rem",
                "overflow": "hidden",
            },
        )

    # noinspection PyMethodMayBeStatic
    def _build_channel_list_table(self, channel: Channel, value: list) -> dbc.Table:
        """One row per list element. Index left, value+unit right-aligned.
        Floats render with fixed 2-decimal precision and tabular-nums so the
        column reads as a clean vertical list."""
        unit = (channel.unit or "").strip()

        def _fmt(v):
            if isinstance(v, float):
                return f"{v:.3f}"
            return str(v)

        idx_style = {
            "padding": "0.15rem 0.5rem",
            "color": "var(--bs-secondary-color, #6c757d)",
            "width": "2.5rem",
        }
        val_style = {
            "padding": "0.15rem 0.5rem",
            "textAlign": "right",
            "fontVariantNumeric": "tabular-nums",
        }

        rows = [
            html.Tr(
                [
                    html.Td(str(i), style=idx_style),
                    html.Td(f"{_fmt(v)} {unit}".rstrip(), style=val_style),
                ]
            )
            for i, v in enumerate(value)
        ]
        th_base = {"padding": "0.15rem 0.5rem", "fontWeight": "normal"}
        header = html.Thead(
            html.Tr(
                [
                    html.Th("Index", style={**th_base, "width": "2.5rem"}),
                    html.Th("Value", style={**th_base, "textAlign": "right"}),
                ]
            )
        )
        return dbc.Table(
            [header, html.Tbody(rows)],
            size="sm",
            striped=True,
            bordered=False,
            hover=True,
            className="mb-1",
            style={
                "minWidth": "16rem",
                "width": "auto",
                "border": "1px solid var(--bs-border-color, #dee2e6)",
                "borderRadius": "0.25rem",
                "overflow": "hidden",
            },
        )

    # noinspection PyMethodMayBeStatic
    def _build_channel_timestamp(self, channel: Channel) -> html.Small:
        timestamp = channel.timestamp
        if isinstance(timestamp, pd.Timestamp) and not pd.isna(timestamp):
            timestamp = timestamp.isoformat(sep=" ", timespec="seconds")
        elif timestamp is None or (not isinstance(timestamp, pd.Timestamp) and pd.isna(timestamp)):
            timestamp = "—"
        else:
            timestamp = str(timestamp)
        return html.Small(timestamp, className="text-muted")

    # noinspection PyMethodMayBeStatic
    def _build_channel_value(self, channel: Channel) -> html.Span:
        # TODO: Implement further type validation
        value = channel.value
        if channel.type == list:
            if value is None:
                return html.Span("—", className="text-muted mb-1")
            return html.Span(f"({len(value)} values)", className="text-muted mb-1")
        # ``pd.isna`` returns an ndarray for non-scalar values, which then
        # blows up the truth-value check. Short-circuit on collections.
        if value is None or (not isinstance(value, (str, bytes, bytearray, Collection)) and pd.isna(value)):
            return html.Span("—", className="text-muted mb-1")
        if channel.type == bytes:
            label = format_bytes_label(channel, value)
            return html.Span(label, className="text-muted mb-1")
        # Multi-row channel values (e.g. forecast trajectories the soil
        # predictor publishes as a length-N ``pd.Series`` indexed by target
        # timestamp) cannot go through scalar format specifiers — ``f"{s:#.3g}"``
        # raises ``TypeError: unsupported format string passed to
        # Series.__format__``. Render a length summary plus the most recent
        # entry so the accordion header stays informative.
        if isinstance(value, pd.Series):
            latest = value.dropna()
            if latest.empty:
                summary = f"({len(value)} values)"
            else:
                last = latest.iloc[-1]
                if channel.type == float and isinstance(last, (int, float)):
                    formatted = format_number(last)
                elif isinstance(last, pd.Timestamp):
                    formatted = last.isoformat(sep=" ", timespec="seconds")
                else:
                    formatted = str(last)
                summary = f"({len(value)} values, latest={formatted})"
            return html.Span(summary, className="text-muted mb-1")
        if channel.type == float:
            # Two decimals in the plain range ("0.00", "1234.00"), three
            # significant figures below one, scientific notation outside.
            value = format_number(channel.value)
        if channel.type == bytes:
            value = None
        # React does not render bare bools (False → empty). Stringify so the channel value is always visible.
        return html.Span(str(value), className="mb-1")

    # noinspection PyMethodMayBeStatic
    def _build_channel_unit(self, channel: Channel) -> html.Span:
        return html.Span(channel.unit, className="text-muted", style={"marginLeft": "0.3rem"})

    # noinspection PyMethodMayBeStatic
    def _build_channel_state(self, channel: Channel) -> html.Small:
        state = str(channel.state).replace("_", " ")
        color = "success" if channel.is_valid() else "warning"
        if state.lower().endswith("error") or state.lower() == "disabled":
            color = "danger"
        return html.Small(state.title(), className=f"text-{color}")

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
        fingerprints_id = f"{self.id}-connectors-fingerprints"

        @callback(
            Output(f"{self.id}-connectors", "children"),
            Output(fingerprints_id, "data"),
            Input("view-update", "n_intervals"),
            State(fingerprints_id, "data"),
        )
        def _update(_, shown):
            fingerprints = [_connector_fingerprint(c) for c in connectors]
            if shown == fingerprints:
                return no_update, no_update
            return [self._build_connector_item(c) for c in connectors], fingerprints

        return html.Div(
            [
                dcc.Store(id=fingerprints_id),
                dbc.Accordion(
                    id=f"{self.id}-connectors",
                    children=[self._build_connector_item(c) for c in connectors],
                    start_collapsed=True,
                    always_open=True,
                    flush=True,
                ),
            ]
        )

    def _build_connector_item(self, connector: Connector) -> dbc.AccordionItem:
        href = f"/connector/{self._encode_id(connector.id)}"

        if not connector.is_enabled():
            badge = dbc.Badge("Disabled", color="secondary")
            timestamp_str = "—"
        elif connector._connected:
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
        fingerprints_id = f"{self.id}-components-fingerprints"

        @callback(
            Output(f"{self.id}-components", "children"),
            Output(fingerprints_id, "data"),
            Input("view-update", "n_intervals"),
            State(fingerprints_id, "data"),
        )
        def _update(_, shown):
            fingerprints = [_component_fingerprint(c) for c in components]
            if shown == fingerprints:
                return no_update, no_update
            return [self._build_component_item(c) for c in components], fingerprints

        return html.Div(
            [
                dcc.Store(id=fingerprints_id),
                dbc.Accordion(
                    id=f"{self.id}-components",
                    children=[self._build_component_item(c) for c in components],
                    start_collapsed=True,
                    always_open=True,
                    flush=True,
                ),
            ]
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
