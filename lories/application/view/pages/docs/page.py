# -*- coding: utf-8 -*-
"""
lories.application.view.pages.docs.page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Static reference page listing all registered entity types (connectors,
components) and their declared configurable parameters.

Reads directly from the module-level ``Registry`` singletons — no instance
data required, no callbacks, no intervals.  Zero runtime overhead.
"""

from __future__ import annotations

import inspect
from typing import Any, Dict, List, Tuple

import dash_bootstrap_components as dbc
from dash import html

from lories.application.view.pages.layout import PageLayout
from lories.application.view.pages.page import Page
from lories.core.configs.parameters import ParameterGroup, _Parameter


class DocsPage(Page):
    """Static documentation page for registered entity types."""

    order: int = 999

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(
            id="docs",
            key="docs",
            name="Docs",
            title="Documentation",
            description="Registered entity types and their configurable parameters.",
            *args,
            **kwargs,
        )

    def create_layout(self, layout: PageLayout) -> None:
        layout.card.add_title("Docs")
        layout.card.add_footer(href=self.path)

        layout.append(html.H4("Documentation"))
        layout.append(
            html.P(
                "Reference overview of registered types and their configurable parameters.",
                className="text-muted",
            )
        )
        layout.append(html.Hr())
        layout.append(_build_tabs())


# ---------------------------------------------------------------------------
# Module-level rendering helpers (no instance state needed)
# ---------------------------------------------------------------------------


def _build_tabs() -> dbc.Tabs:
    from lories.components.context import registry as component_registry
    from lories.connectors.context import registry as connector_registry

    return dbc.Tabs(
        [
            dbc.Tab(
                label="Connectors",
                tab_id="tab-connectors",
                children=_build_registry_section(connector_registry, "connector"),
            ),
            dbc.Tab(
                label="Components",
                tab_id="tab-components",
                children=_build_registry_section(component_registry, "component"),
            ),
        ],
        active_tab="tab-connectors",
        className="mt-3",
    )


def _build_registry_section(registry, category: str) -> html.Div:
    type_keys = sorted(registry.get_types())
    if not type_keys:
        return html.Div(
            html.I(f"No {category} types registered.", className="text-muted"),
            className="mt-2",
        )

    items = [_build_type_item(registry.from_type(key), category) for key in type_keys]
    return html.Div(
        dbc.Accordion(items, start_collapsed=True, always_open=True, flush=True),
        className="mt-2",
    )


def _build_type_item(registration, category: str) -> dbc.AccordionItem:
    cls = registration.type

    # Title: key (monospace) + class name + alias badges
    title_children: List[Any] = [
        html.Code(registration.key, className="me-2"),
        html.Small(cls.__name__, className="text-muted me-2"),
    ]
    for alias in registration.alias:
        title_children.append(dbc.Badge(alias, color="secondary", pill=True, className="ms-1"))

    body: List[Any] = []

    # Module path + parent classes — always shown for context
    parents = [c.__name__ for c in cls.__mro__[1:] if c is not object]
    meta_parts: List[Any] = [
        html.Small(cls.__module__, className="text-muted font-monospace me-3"),
    ]
    if parents:
        meta_parts.append(
            html.Small(
                ["extends: "] + [html.Code(p, className="me-1") for p in parents],
                className="text-muted",
            )
        )
    body.append(html.Div(meta_parts, className="mb-2 d-flex flex-wrap gap-2 align-items-center"))

    # Docstring — first paragraph only
    doc = inspect.getdoc(cls)
    if doc:
        first_para = doc.split("\n\n")[0].replace("\n", " ")
        body.append(html.P(first_para, className="text-muted fst-italic small mb-2"))

    # Config parameters (class-level _Parameter declarations)
    config_params: Dict[str, _Parameter] = getattr(cls, "__config_parameters__", {})
    if config_params:
        body.append(html.H6("Config Parameters", className="mt-2 mb-1 text-secondary"))
        body.append(_render_config_params(config_params))

    # Channel / resource parameters (connectors only, ResourceParameter)
    channel_params = getattr(cls, "__channel_parameters__", {})
    if channel_params:
        body.append(html.H6("Channel Parameters", className="mt-2 mb-1 text-secondary"))
        body.append(_render_channel_params(channel_params))

    if not config_params and not channel_params:
        body.append(dbc.Badge("no configurable parameters", color="light", text_color="secondary", className="me-1"))

    return dbc.AccordionItem(
        title=dbc.Row(
            dbc.Col(title_children, className="d-flex align-items-center flex-wrap gap-1"),
            className="w-100",
        ),
        children=body,
        item_id=f"docs-{category}-{registration.key}",
    )


# ---------------------------------------------------------------------------
# Parameter rendering
# ---------------------------------------------------------------------------


def _render_config_params(params: Dict[str, _Parameter]) -> html.Div:
    """Render _Parameter / ParameterGroup declarations as readable tables."""
    flat: List[Tuple[str, dict]] = []
    groups: List[Tuple[str, _Parameter]] = []

    for attr, param in params.items():
        if isinstance(param, ParameterGroup):
            groups.append((attr, param))
        else:
            flat.append((attr, param.to_schema()))

    children: List[Any] = []

    if flat:
        children.append(_param_table(flat))

    for _attr, group in groups:
        schema = group.to_schema()
        key = schema.get("key", _attr)
        desc = schema.get("desc") or ""
        required = schema.get("required", False)

        header: List[Any] = [html.Code(key)]
        if required:
            header.append(dbc.Badge("required", color="warning", pill=True, className="ms-1"))
        if desc:
            header.append(html.Small(f" — {desc}", className="text-muted ms-1 small"))

        nested_schemas: dict = schema.get("children", {})
        if nested_schemas:
            group_body: Any = html.Div(
                _param_table(list(nested_schemas.items())),
                style={"marginLeft": "1rem"},
            )
        else:
            group_body = html.I("No child parameters.", className="text-muted small")

        children.append(
            html.Div(
                [html.Div(header, className="mb-1"), group_body],
                className="mb-2",
            )
        )

    return html.Div(children)


def _render_channel_params(params: dict) -> dbc.Table:
    """Render ResourceParameter entries as a compact table."""
    rows = []
    for _attr, param in params.items():
        key = param._resolve_key()
        type_label = param.type.__name__ if getattr(param, "type", None) else ""
        required = getattr(param, "required", False)
        has_default = getattr(param, "_has_default", False)
        default = param.default if has_default else None
        desc = getattr(param, "desc", "") or ""
        choices = getattr(param, "choices", None)

        badges: List[Any] = []
        if type_label:
            badges.append(dbc.Badge(type_label, color="info", pill=True, className="ms-1"))
        if required:
            badges.append(dbc.Badge("required", color="warning", pill=True, className="ms-1"))
        elif has_default and default is not None:
            badges.append(
                dbc.Badge(f"default: {default}", color="light", text_color="dark", pill=True, className="ms-1")
            )

        desc_parts: List[Any] = []
        if desc:
            desc_parts.append(html.Small(desc, className="text-muted fst-italic"))
        if choices:
            desc_parts.append(
                html.Small(
                    [" — one of: ", html.Code(", ".join(str(c) for c in choices))],
                    className="text-muted",
                )
            )

        rows.append(
            html.Tr(
                [
                    html.Td(
                        [html.Code(key)] + badges,
                        style={"whiteSpace": "nowrap", "width": "1%", "paddingRight": "1rem"},
                    ),
                    html.Td(desc_parts or ""),
                ]
            )
        )

    return dbc.Table(
        html.Tbody(rows),
        bordered=True,
        hover=True,
        size="sm",
        className="mb-2",
        style={"fontSize": "0.85rem"},
    )


def _param_table(items: List[Tuple[str, dict]]) -> dbc.Table:
    """Render a list of (key_or_attr, schema_dict) pairs as a parameter table.

    ``schema_dict`` is the output of ``_Parameter.to_schema()``.
    """
    rows = []
    for attr_or_key, schema in items:
        key = schema.get("key", attr_or_key)
        type_label = schema.get("type", "")
        required = schema.get("required", False)
        default = schema.get("default")
        desc = schema.get("desc") or ""
        choices = schema.get("choices")

        badges: List[Any] = []
        if type_label:
            badges.append(dbc.Badge(type_label, color="info", pill=True, className="ms-1"))
        if required:
            badges.append(dbc.Badge("required", color="warning", pill=True, className="ms-1"))
        elif default is not None:
            badges.append(
                dbc.Badge(f"default: {default}", color="light", text_color="dark", pill=True, className="ms-1")
            )

        desc_parts: List[Any] = []
        if desc:
            desc_parts.append(html.Small(desc, className="text-muted fst-italic"))
        if choices:
            desc_parts.append(
                html.Small(
                    [" — one of: ", html.Code(", ".join(str(c) for c in choices))],
                    className="text-muted",
                )
            )

        rows.append(
            html.Tr(
                [
                    html.Td(
                        [html.Code(key)] + badges,
                        style={"whiteSpace": "nowrap", "width": "1%", "paddingRight": "1rem"},
                    ),
                    html.Td(desc_parts or ""),
                ]
            )
        )

    return dbc.Table(
        html.Tbody(rows),
        bordered=True,
        hover=True,
        size="sm",
        className="mb-2",
        style={"fontSize": "0.85rem"},
    )
