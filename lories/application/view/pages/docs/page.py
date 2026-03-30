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
from dash import dcc, html

from lories.application.view.pages.layout import PageLayout
from lories.application.view.pages.page import Page
from lories.core.configs.parameters import _Parameter


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
    from lories.data.database import Database

    connector_types, database_types = _split_registry(connector_registry, Database)

    return dbc.Tabs(
        [
            dbc.Tab(
                label="Connectors",
                tab_id="tab-connectors",
                children=_build_type_section(connector_types, "connector"),
            ),
            dbc.Tab(
                label="Databases",
                tab_id="tab-databases",
                children=_build_type_section(database_types, "database"),
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


def _split_registry(registry, database_cls) -> Tuple[list, list]:
    """Split registry entries into connectors and databases based on inheritance."""
    connectors = []
    databases = []
    for key in sorted(registry.get_types()):
        registration = registry.from_type(key)
        if issubclass(registration.type, database_cls):
            databases.append(registration)
        else:
            connectors.append(registration)
    return connectors, databases


def _build_type_section(registrations: list, category: str) -> html.Div:
    """Build an accordion section from a pre-filtered list of registrations."""
    if not registrations:
        return html.Div(
            html.I(f"No {category} types registered.", className="text-muted"),
            className="mt-2",
        )

    items = [_build_type_item(reg, category) for reg in registrations]
    return html.Div(
        dbc.Accordion(items, start_collapsed=True, always_open=True, flush=True),
        className="mt-2",
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


def _clean_class_name(name: str) -> str:
    """Remove trailing 'Connector' or 'Database' from a class name."""
    for suffix in ("Connector", "Database"):
        if name.endswith(suffix) and name != suffix:
            return name[: -len(suffix)]
    return name


def _build_type_item(registration, category: str) -> dbc.AccordionItem:
    cls = registration.type

    # Title: class name + key badge + alias badges (key and aliases are both valid keys)
    title_children: List[Any] = [
        html.Span(_clean_class_name(cls.__name__), className="me-2"),
        dbc.Badge(registration.key, color="primary", pill=True, className="ms-1"),
    ]
    for alias in registration.alias:
        title_children.append(dbc.Badge(alias, color="secondary", pill=True, className="ms-1"))
    if not registration.available:
        title_children.append(dbc.Badge("not installed", color="danger", pill=True, className="ms-1"))

    body: List[Any] = []

    if not registration.available:
        body.append(
            dbc.Alert(
                [html.Strong("Unavailable: "), registration.error or "Missing dependency."],
                color="warning",
                className="mb-2 py-2",
            )
        )

    # Docstring — full text rendered as Markdown
    doc = inspect.getdoc(cls)
    if doc:
        body.append(dcc.Markdown(doc, className="text-muted small mb-2"))

    # Module path + parent classes
    parents = [c.__name__ for c in cls.__mro__[1:] if c is not object]
    meta_parts: List[Any] = [
        html.Small(cls.__module__, className="text-muted font-monospace me-3"),
    ]
    if parents:
        meta_parts.append(html.Small("extends:", className="text-muted me-1"))
        # Group public/private pairs (e.g. Database + _Database) into one badge
        grouped: List[Any] = []
        skip: set = set()
        for i, p in enumerate(parents):
            if i in skip:
                continue
            private_name = f"_{p}"
            # Check if the next entry is the _-prefixed counterpart
            if i + 1 < len(parents) and parents[i + 1] == private_name:
                skip.add(i + 1)
                grouped.append(
                    dbc.Badge(
                        [html.Div(p, className="text-end"), html.Div(private_name, className="text-end opacity-50")],
                        color="light",
                        text_color="dark",
                        pill=True,
                        className="ms-1 px-3 py-1",
                    )
                )
            else:
                grouped.append(dbc.Badge(p, color="light", text_color="dark", pill=True, className="ms-1"))
        meta_parts.extend(grouped)
    body.append(html.Div(meta_parts, className="mb-2 d-flex flex-wrap gap-2 align-items-center"))

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
    schemas = {attr: param.to_schema() for attr, param in params.items()}
    return _render_schema_items(schemas)


def _render_schema_items(schemas: dict) -> html.Div:
    """Recursively render a dict of ``{key: schema_dict}`` pairs, nesting groups."""
    flat: List[Tuple[str, dict]] = []
    groups: List[Tuple[str, dict]] = []

    for attr_or_key, schema in schemas.items():
        if schema.get("type") == "group":
            groups.append((attr_or_key, schema))
        else:
            flat.append((attr_or_key, schema))

    children: List[Any] = []

    if flat:
        children.append(_param_table(flat))

    for attr_or_key, schema in groups:
        key = schema.get("key", attr_or_key)
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
                _render_schema_items(nested_schemas),
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
