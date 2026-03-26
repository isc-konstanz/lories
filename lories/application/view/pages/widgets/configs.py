# -*- coding: utf-8 -*-
"""
lories.application.view.pages.widgets.configs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Reusable configuration display widget for Dash pages.
Renders declared Parameters on top with metadata, and
undeclared config values below with a red-tinted background.
Nested config sections are rendered as collapsible cards.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Dict, List, Optional

import dash_bootstrap_components as dbc
from dash import html

from lories.core.configs.parameters import _Parameter, ParameterGroup


def build_configs_widget(
    configs,
    configurator_type: type,
    *,
    prefix: str = "",
) -> html.Div:
    """Build a complete configs display for a configurator instance.

    Parameters
    ----------
    configs
        The ``Configurations`` instance (``MutableMapping``-like).
    configurator_type
        The class of the configurator (used to read ``__config_parameters__``).
    prefix
        An optional prefix for accordion ``item_id`` values to avoid collisions.
    """
    if not configs:
        return html.Div(html.I("No configurations.", className="text-muted"))

    params: Dict[str, _Parameter] = getattr(configurator_type, "__config_parameters__", {})
    param_keys = {p._resolve_key() for p in params.values() if not isinstance(p, ParameterGroup)}
    param_group_keys = {p._resolve_key() for p in params.values() if isinstance(p, ParameterGroup)}

    declared = [(k, v) for k, v in configs.items() if k in param_keys]
    undeclared_flat = [
        (k, v) for k, v in configs.items()
        if k not in param_keys and k not in param_group_keys and not isinstance(v, Mapping)
    ]
    undeclared_sections = [
        (k, v) for k, v in configs.items()
        if k not in param_keys and k not in param_group_keys and isinstance(v, Mapping)
    ]

    param_by_key = {p._resolve_key(): p for p in params.values()}

    children = []

    # -- Declared parameters (with schema metadata) on top --------------------
    if declared:
        children.append(_render_param_table(declared, param_by_key))

    # -- Parameter groups as nested accordions --------------------------------
    for gkey in param_group_keys:
        if configs.has_member(gkey):
            member = configs.get_member(gkey)
            children.append(
                _render_section_card(gkey, member, prefix=f"{prefix}{gkey}-", depth=0)
            )

    # -- Undeclared sections --------------------------------------------------
    if undeclared_sections:
        for key, section in undeclared_sections:
            children.append(
                _render_section_card(
                    key, section, prefix=f"{prefix}{key}-", depth=0,
                    header_color="rgba(220, 53, 69, 0.06)",
                )
            )

    # -- Undeclared flat config values (red tinted) on bottom -----------------
    if undeclared_flat:
        children.append(html.Hr(className="my-2"))
        children.append(
            html.Div(
                [
                    html.Small("Unregistered", className="text-muted d-block mb-1"),
                    _render_plain_table(undeclared_flat),
                ],
                style={
                    "backgroundColor": "rgba(220, 53, 69, 0.06)",
                    "borderRadius": "0.25rem",
                    "padding": "0.5rem",
                },
            )
        )

    return html.Div(children)


# -- Rendering helpers --------------------------------------------------------


def _render_param_table(items: List[tuple], param_by_key: Dict[str, _Parameter]) -> dbc.Table:
    rows = []
    for key, value in items:
        param = param_by_key.get(key)
        schema = param.to_schema() if param else {}
        type_label = schema.get("type", "")
        default = schema.get("default")
        desc = schema.get("desc") or ""

        badge_children = []
        if type_label:
            badge_children.append(dbc.Badge(type_label, color="info", className="ms-1", pill=True))
        if schema.get("required"):
            badge_children.append(dbc.Badge("required", color="warning", className="ms-1", pill=True))

        meta_parts = []
        if default is not None:
            meta_parts.append(html.Small(f"default: {default}", className="text-muted me-2"))
        if desc:
            meta_parts.append(html.Small(desc, className="text-muted fst-italic"))

        rows.append(
            html.Tr([
                html.Td(
                    [html.Code(key)] + badge_children,
                    style={"whiteSpace": "nowrap", "width": "1%", "paddingRight": "1rem"},
                ),
                html.Td(html.Span(str(value)), style={"fontFamily": "monospace"}),
                html.Td(meta_parts, style={"whiteSpace": "nowrap"}),
            ])
        )
    return dbc.Table(
        html.Tbody(rows),
        bordered=True,
        hover=True,
        size="sm",
        className="mb-2",
        style={"fontSize": "0.85rem"},
    )


def _render_plain_table(items: List[tuple]) -> dbc.Table:
    return dbc.Table(
        html.Tbody([
            html.Tr([
                html.Td(
                    html.Code(key, style={"fontSize": "0.8rem"}),
                    style={"whiteSpace": "nowrap", "width": "1%", "paddingRight": "1rem"},
                ),
                html.Td(
                    html.Small(str(value)),
                    style={"fontFamily": "monospace"},
                ),
            ])
            for key, value in items
        ]),
        bordered=True,
        hover=True,
        size="sm",
        className="mb-0",
        style={"fontSize": "0.85rem"},
    )


def _render_section_card(
    title: str,
    configs: Mapping,
    *,
    prefix: str = "",
    depth: int = 0,
    header_color: Optional[str] = None,
) -> dbc.Accordion:
    """Render a nested config section as a foldable accordion item."""
    flat_items = [(k, v) for k, v in configs.items() if not isinstance(v, Mapping)]
    sub_sections = [(k, v) for k, v in configs.items() if isinstance(v, Mapping)]

    body_children = []

    if flat_items:
        body_children.append(_render_plain_table(flat_items))

    if sub_sections:
        for key, section in sub_sections:
            body_children.append(
                _render_section_card(
                    key, section,
                    prefix=f"{prefix}{key}-",
                    depth=depth + 1,
                    header_color=header_color,
                )
            )

    if not body_children:
        body_children.append(html.I("Empty section.", className="text-muted"))

    item_id = f"{prefix}section-{title}"

    return dbc.Accordion(
        dbc.AccordionItem(
            title=html.Code(title, style={"fontSize": "0.85rem"}),
            children=body_children,
            item_id=item_id,
            style={"backgroundColor": header_color} if header_color else {},
        ),
        start_collapsed=(depth > 0),
        active_item=item_id if depth == 0 else None,
        always_open=True,
        flush=True,
        className="mb-1",
        style={"marginLeft": f"{depth * 0.5}rem"},
    )
