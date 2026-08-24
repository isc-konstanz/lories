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
from typing import Any, Dict, List, Optional, Tuple

import dash_bootstrap_components as dbc
from dash import dcc, html

from lories._core import _Constant
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
    available = getattr(registration, "available", True)
    if not available:
        title_children.append(dbc.Badge("not installed", color="danger", pill=True, className="ms-1"))

    body: List[Any] = []

    if not available:
        body.append(
            dbc.Alert(
                [html.Strong("Unavailable: "), getattr(registration, "error", None) or "Missing dependency."],
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
        body.append(_section_heading("Config Parameters"))
        body.append(_render_config_params(config_params))

    # Channel / resource parameters (connectors only, ResourceParameter)
    channel_params = getattr(cls, "__channel_parameters__", {})
    if channel_params:
        body.append(_section_heading("Channel Parameters"))
        body.append(_render_channel_params(channel_params))

    # Constants — class-level Constant declarations (data channels the entity exposes)
    constants = _collect_constants(cls)
    if constants:
        body.append(_section_heading("Channels"))
        body.append(_render_constants(constants))

    if not config_params and not channel_params and not constants:
        body.append(dbc.Badge("no configurable parameters", color="light", text_color="secondary", className="me-1"))

    return dbc.AccordionItem(
        title=dbc.Row(
            dbc.Col(title_children, className="d-flex align-items-center flex-wrap gap-1"),
            className="w-100",
        ),
        children=body,
        item_id=f"docs-{category}-{registration.key}",
    )


def _section_heading(label: str) -> html.Div:
    """Render a prominent section divider so subsections (Config Parameters, Channels, …) stand out."""
    return html.Div(
        html.H5(
            label,
            className="mb-0 text-uppercase fw-semibold text-body-emphasis",
            style={"letterSpacing": "0.05em", "fontSize": "0.95rem"},
        ),
        className="mt-4 mb-2 pt-2 border-top",
    )


def _render_config_params(params: Dict[str, _Parameter]) -> html.Div:
    """Render _Parameter / ParameterGroup declarations as readable tables."""
    schemas = {attr: param.to_schema() for attr, param in params.items()}
    return _render_schema_items(schemas)


def _render_schema_items(schemas: dict) -> html.Div:
    """Recursively render a dict of ``{key: schema_dict}`` pairs, nesting groups."""
    flat: List[Tuple[str, dict]] = []
    groups: List[Tuple[str, dict]] = []
    entities: List[Tuple[str, dict]] = []

    for attr_or_key, schema in schemas.items():
        t = schema.get("type")
        if t == "group":
            groups.append((attr_or_key, schema))
        elif t in ("component", "connector"):
            entities.append((attr_or_key, schema))
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

    for attr_or_key, schema in entities:
        children.append(_render_entity_slot(attr_or_key, schema))

    return html.Div(children)


def _render_entity_slot(attr_or_key: str, schema: dict) -> html.Div:
    """Render a ComponentParameter / ConnectorParameter schema entry."""
    key = schema.get("key", attr_or_key)
    kind = schema.get("type", "entity")
    cls_name = schema.get("cls")
    desc = schema.get("desc") or ""
    required = schema.get("required", False)
    multiple = schema.get("multiple", False)
    fitting = schema.get("fitting_types") or []

    header: List[Any] = [html.Code(key)]
    header.append(
        dbc.Badge(
            f"{kind}{'[]' if multiple else ''}",
            color="info" if kind == "component" else "primary",
            pill=True,
            className="ms-1",
        )
    )
    if cls_name:
        header.append(dbc.Badge(cls_name, color="light", text_color="dark", pill=True, className="ms-1"))
    if required:
        header.append(dbc.Badge("required", color="warning", pill=True, className="ms-1"))
    if desc:
        header.append(html.Small(f" — {desc}", className="text-muted ms-1 small"))

    body: List[Any] = []

    if fitting:
        fitting_badges: List[Any] = []
        for entry in fitting:
            label_parts: List[Any] = [entry["key"]]
            if not entry.get("available", True):
                fitting_badges.append(
                    dbc.Badge(
                        label_parts + [" (unavailable)"],
                        color="light",
                        text_color="danger",
                        pill=True,
                        className="me-1 mb-1",
                    )
                )
            else:
                fitting_badges.append(
                    dbc.Badge(
                        label_parts,
                        color="light",
                        text_color="dark",
                        pill=True,
                        className="me-1 mb-1",
                    )
                )
        body.append(
            html.Div(
                [
                    html.Small("fitting types: ", className="text-muted me-1"),
                    html.Div(fitting_badges, className="d-inline"),
                ],
                className="mb-2",
            )
        )
    elif cls_name:
        body.append(html.Small("no registered fitting types", className="text-muted d-block mb-2"))

    nested_schemas: dict = schema.get("children", {})
    if nested_schemas:
        body.append(
            html.Div(
                _render_schema_items(nested_schemas),
                style={"marginLeft": "1rem"},
            )
        )
    else:
        body.append(html.I("No child parameters.", className="text-muted small"))

    return html.Div(
        [html.Div(header, className="mb-1"), html.Div(body)],
        className="mb-3 ps-2 border-start border-2",
    )


_NOWRAP = {"whiteSpace": "nowrap"}
_MONO = {"fontFamily": "monospace"}
_MUTED = {"color": "var(--bs-secondary-color)"}
_FIT = {"whiteSpace": "nowrap", "width": "1%"}

_COL_MIN_WIDTHS = {
    "Name": "12rem",
    "Type": "6rem",
    "Required": "6rem",
    "Default": "10rem",
    "Choices": "20rem",
}


def _fit_with_min(col: str) -> dict:
    return {**_FIT, "minWidth": _COL_MIN_WIDTHS[col]}


def _param_header() -> html.Thead:
    return html.Thead(
        html.Tr(
            [
                html.Th("Name", style=_fit_with_min("Name")),
                html.Th("Description"),
                html.Th("Type", style=_fit_with_min("Type")),
                html.Th("Required", style=_fit_with_min("Required")),
                html.Th("Default", style=_fit_with_min("Default")),
                html.Th("Choices", style=_fit_with_min("Choices")),
            ]
        )
    )


def _param_row(
    key: str,
    type_label: str,
    required: bool,
    default: Any,
    has_default: bool,
    desc: str,
    choices: Optional[List[Any]],
) -> html.Tr:
    default_cell: Any
    if has_default:
        default_cell = str(default)
    elif required:
        default_cell = html.Span("—", style=_MUTED)
    else:
        default_cell = html.Span("—", style=_MUTED)

    choices_cell: Any = ""
    if choices:
        choices_cell = html.Code(", ".join(str(c) for c in choices), style={"fontSize": "0.8rem"})

    return html.Tr(
        [
            html.Td(html.Code(key), style={**_FIT, "paddingRight": "1rem"}),
            html.Td(html.Small(desc, style=_MUTED) if desc else "", style={"fontStyle": "italic"}),
            html.Td(type_label, style={**_FIT, **_MONO}),
            html.Td("yes" if required else html.Span("no", style=_MUTED), style=_FIT),
            html.Td(default_cell, style={**_FIT, **_MONO}),
            html.Td(choices_cell, style=_FIT),
        ]
    )


def _collect_constants(cls: type) -> List[Tuple[str, _Constant]]:
    """Walk the MRO and collect Constant class attributes, deduped by id."""
    seen: set = set()
    result: List[Tuple[str, _Constant]] = []
    for klass in cls.__mro__:
        for attr, value in vars(klass).items():
            if not isinstance(value, _Constant):
                continue
            if value.id in seen:
                continue
            seen.add(value.id)
            result.append((attr, value))
    return result


def _render_constants(items: List[Tuple[str, _Constant]]) -> dbc.Table:
    """Render Constant declarations as a compact table."""
    header = html.Thead(
        html.Tr(
            [
                html.Th("Name", style=_fit_with_min("Name")),
                html.Th("Description"),
                html.Th("Type", style=_fit_with_min("Type")),
                html.Th("Unit", style=_fit_with_min("Required")),
            ]
        )
    )

    rows = []
    for _attr, constant in items:
        type_label = constant.type.__name__ if getattr(constant, "type", None) else ""
        unit = constant.unit or ""
        name = constant.name or ""
        rows.append(
            html.Tr(
                [
                    html.Td(html.Code(constant.id), style={**_FIT, "paddingRight": "1rem"}),
                    html.Td(html.Small(name, style=_MUTED) if name else "", style={"fontStyle": "italic"}),
                    html.Td(type_label, style={**_FIT, **_MONO}),
                    html.Td(unit if unit else html.Span("—", style=_MUTED), style=_FIT),
                ]
            )
        )

    return dbc.Table(
        [header, html.Tbody(rows)],
        bordered=True,
        hover=True,
        size="sm",
        className="mb-2",
        style={"fontSize": "0.85rem"},
    )


def _render_channel_params(params: dict) -> dbc.Table:
    """Render ResourceParameter entries as a compact table."""
    rows = []
    for _attr, param in params.items():
        rows.append(
            _param_row(
                key=param._resolve_key(),
                type_label=param.type.__name__ if getattr(param, "type", None) else "",
                required=bool(getattr(param, "required", False)),
                default=param.default if getattr(param, "_has_default", False) else None,
                has_default=bool(getattr(param, "_has_default", False)),
                desc=getattr(param, "desc", "") or "",
                choices=getattr(param, "choices", None),
            )
        )

    return dbc.Table(
        [_param_header(), html.Tbody(rows)],
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
        rows.append(
            _param_row(
                key=schema.get("key", attr_or_key),
                type_label=schema.get("type", "") or "",
                required=bool(schema.get("required", False)),
                default=schema.get("default"),
                has_default=bool(schema.get("has_default"))
                if "has_default" in schema
                else (schema.get("default") is not None),
                desc=schema.get("desc") or "",
                choices=schema.get("choices"),
            )
        )

    return dbc.Table(
        [_param_header(), html.Tbody(rows)],
        bordered=True,
        hover=True,
        size="sm",
        className="mb-2",
        style={"fontSize": "0.85rem"},
    )
