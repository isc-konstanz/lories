# -*- coding: utf-8 -*-
"""
lories.application.view.pages.widgets.configs_editor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Config editor modal for Dash pages.

Renders a per-entity "Edit Configs" button that opens a ``dbc.Modal`` with:
  - Editable input fields for every declared ``_Parameter``
  - Enable/disable toggles for sub-components and connectors
  - Save  → writes changes to disk via ``configs.write()``
  - Discard → closes without any changes

Sub-component and connector sections show enable/disable toggles only;
add/remove is not yet supported.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Tuple

import dash_bootstrap_components as dbc
from dash import ALL, Input, Output, State, callback, ctx, html

from lories.core.configs.parameters import (
    BoolParameter,
    ParameterGroup,
    _EntityParameter,
    _Parameter,
)


def build_configs_editor_modal(
    entity_id: str,
    configs,
    configurator_type: type,
    *,
    components: Optional[List] = None,
    connectors: Optional[List] = None,
) -> Tuple[dbc.Button, dbc.Modal]:
    """Return *(open_button, modal)* for editing *configs* in a dialog.

    Parameters
    ----------
    entity_id:
        Unique, already-encoded ID for the entity.  Used as a prefix for all
        Dash component IDs and callback IDs created here.
    configs:
        The live ``Configurations`` instance to read from and write to.
    configurator_type:
        The class of the configurator — provides ``__config_parameters__``.
    components:
        Optional list of sub-``Component`` objects; shown as toggleable rows.
    connectors:
        Optional list of ``Connector`` objects; shown as toggleable rows.
    """
    open_button = dbc.Button(
        "Edit",
        id=f"{entity_id}-config-open-btn",
        color="secondary",
        outline=True,
        size="sm",
    )

    modal = dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle("Edit Configuration")),
            dbc.ModalBody(
                [
                    html.Div(id=f"{entity_id}-config-feedback"),
                    _build_modal_body(
                        configs,
                        configurator_type,
                        entity_id,
                        components,
                        connectors,
                    ),
                ]
            ),
            dbc.ModalFooter(
                [
                    dbc.Button(
                        "Save",
                        id=f"{entity_id}-config-save-btn",
                        color="primary",
                        className="me-2",
                    ),
                    dbc.Button(
                        "Discard",
                        id=f"{entity_id}-config-discard-btn",
                        color="secondary",
                        outline=True,
                    ),
                ]
            ),
        ],
        id=f"{entity_id}-config-modal",
        is_open=False,
        size="lg",
        scrollable=True,
    )

    _register_callbacks(entity_id, configs, components, connectors)

    return open_button, modal


def _build_modal_body(
    configs,
    configurator_type: type,
    entity_id: str,
    components: Optional[List],
    connectors: Optional[List],
) -> html.Div:
    sections = []

    # Parameters — render directly, no wrapping accordion
    params: Dict[str, _Parameter] = getattr(configurator_type, "__config_parameters__", {})
    sections.append(_build_param_fields(configs, params, entity_id))

    # Components section (keep accordion for these)
    if components is not None:
        sections.append(
            dbc.Accordion(
                dbc.AccordionItem(
                    title="Components",
                    children=_build_entity_toggles(components, entity_id, "comp"),
                    item_id=f"{entity_id}-editor-comps",
                ),
                active_item=f"{entity_id}-editor-comps",
                always_open=True,
                className="mb-2",
            )
        )

    # Connectors section (keep accordion for these)
    if connectors is not None:
        sections.append(
            dbc.Accordion(
                dbc.AccordionItem(
                    title="Connectors",
                    children=_build_entity_toggles(connectors, entity_id, "conn"),
                    item_id=f"{entity_id}-editor-conns",
                ),
                active_item=f"{entity_id}-editor-conns",
                always_open=True,
                className="mb-2",
            )
        )

    return html.Div(sections)


def _build_param_fields(configs, params: Dict[str, _Parameter], entity_id: str) -> html.Div:
    """Build form rows for declared parameters and undeclared flat config keys."""
    if not configs and not params:
        return html.Div(html.I("No parameters.", className="text-muted"))

    param_by_key: Dict[str, _Parameter] = {
        p._resolve_key(): p for p in params.values() if not isinstance(p, (ParameterGroup, _EntityParameter))
    }
    group_keys = {p._resolve_key() for p in params.values() if isinstance(p, ParameterGroup)}
    entity_params: Dict[str, _EntityParameter] = {
        p._resolve_key(): p for p in params.values() if isinstance(p, _EntityParameter)
    }

    rows: List = []

    # Declared parameters
    for key, param in param_by_key.items():
        current_value = configs.get(key) if (configs and key in configs) else None
        if current_value is None and hasattr(param, "default"):
            current_value = param.default
        rows.append(_build_field_row(entity_id, key, param, current_value))

    # Undeclared flat keys (skip nested mappings, group keys, entity slots)
    if configs:
        skip_keys = set(param_by_key) | group_keys | set(entity_params)
        for key, value in configs.items():
            if key not in skip_keys and not isinstance(value, Mapping):
                rows.append(_build_plain_field_row(entity_id, key, value))

    # ParameterGroup children as indented sub-sections
    for gkey in group_keys:
        if configs and configs.has_member(gkey):
            member = configs.get_member(gkey)
            group_param: Optional[ParameterGroup] = next(
                (p for p in params.values() if isinstance(p, ParameterGroup) and p._resolve_key() == gkey),
                None,
            )
            children_params: Dict[str, _Parameter] = group_param.children if group_param else {}
            rows.append(
                html.Div(
                    [
                        html.Hr(className="mt-3 mb-2"),
                        html.Div(
                            [
                                html.Span(gkey, className="fw-semibold text-secondary"),
                                html.Small(" — nested config", className="text-muted ms-1"),
                            ],
                            className="mb-2",
                        ),
                        html.Div(
                            _build_param_fields(member, children_params, f"{entity_id}-{gkey}"),
                            className="ps-3 border-start border-2",
                        ),
                    ]
                )
            )

    # Entity slots (ComponentParameter / ConnectorParameter) — declarative-only.
    # The actual children live in dynamically-loaded sub-configs, so we just
    # surface the schema (slot type, multiplicity, fitting registered types,
    # and recursive child parameters of the expected class).
    for ekey, eparam in entity_params.items():
        rows.append(_build_entity_slot_row(eparam, ekey, entity_id))

    if not rows:
        return html.Div(html.I("No parameters.", className="text-muted"))

    return html.Div(rows)


def _build_entity_slot_row(eparam: _EntityParameter, key: str, entity_id: str) -> html.Div:
    """Render a declarative ComponentParameter / ConnectorParameter slot."""
    schema = eparam.to_schema()
    kind = schema.get("type", "entity")
    multiple = schema.get("multiple", False)
    cls_name = schema.get("cls")
    desc = schema.get("desc") or ""
    fitting = schema.get("fitting_types") or []

    header_parts: List[Any] = [
        html.Span(key, className="fw-semibold text-secondary"),
        dbc.Badge(
            f"{kind}{'[]' if multiple else ''}",
            color="info" if kind == "component" else "primary",
            pill=True,
            className="ms-2",
        ),
    ]
    if cls_name:
        header_parts.append(dbc.Badge(cls_name, color="light", text_color="dark", pill=True, className="ms-1"))
    if desc:
        header_parts.append(html.Small(f" — {desc}", className="text-muted ms-2"))

    body: List[Any] = []
    if fitting:
        badges = [
            dbc.Badge(
                entry["key"],
                color="light",
                text_color="dark" if entry.get("available", True) else "danger",
                pill=True,
                className="me-1 mb-1",
            )
            for entry in fitting
        ]
        body.append(
            html.Div(
                [html.Small("fitting types: ", className="text-muted me-1")] + badges,
                className="mb-1",
            )
        )

    nested = schema.get("children") or {}
    if nested:
        body.append(html.Small("expected child parameters:", className="text-muted d-block"))
        body.append(
            html.Div(
                _build_nested_schema_readonly(nested),
                className="ps-3 border-start border-2 small",
            )
        )

    return html.Div(
        [
            html.Hr(className="mt-3 mb-2"),
            html.Div(header_parts, className="mb-2"),
            html.Div(body),
        ]
    )


def _build_nested_schema_readonly(schemas: Dict[str, Any]) -> html.Div:
    """Read-only rendering of a nested schema dict (used inside entity slots)."""
    rows: List[Any] = []
    for attr_or_key, schema in schemas.items():
        t = schema.get("type")
        key = schema.get("key", attr_or_key)
        if t in ("component", "connector"):
            cls_name = schema.get("cls")
            multiple = schema.get("multiple", False)
            label_parts: List[Any] = [
                html.Code(key),
                dbc.Badge(
                    f"{t}{'[]' if multiple else ''}",
                    color="info" if t == "component" else "primary",
                    pill=True,
                    className="ms-1",
                ),
            ]
            if cls_name:
                label_parts.append(dbc.Badge(cls_name, color="light", text_color="dark", pill=True, className="ms-1"))
            rows.append(html.Div(label_parts, className="mb-1"))
            nested = schema.get("children") or {}
            if nested:
                rows.append(
                    html.Div(
                        _build_nested_schema_readonly(nested),
                        className="ps-3 border-start border-2",
                    )
                )
        elif t == "group":
            label_parts = [
                html.Code(key),
                dbc.Badge("group", color="secondary", pill=True, className="ms-1"),
            ]
            rows.append(html.Div(label_parts, className="mb-1"))
            nested = schema.get("children") or {}
            if nested:
                rows.append(
                    html.Div(
                        _build_nested_schema_readonly(nested),
                        className="ps-3 border-start border-2",
                    )
                )
        else:
            badges: List[Any] = [html.Code(key)]
            if t:
                badges.append(dbc.Badge(t, color="info", pill=True, className="ms-1"))
            if schema.get("required"):
                badges.append(dbc.Badge("required", color="warning", pill=True, className="ms-1"))
            elif schema.get("default") is not None:
                badges.append(
                    dbc.Badge(
                        f"default: {schema['default']}",
                        color="light",
                        text_color="dark",
                        pill=True,
                        className="ms-1",
                    )
                )
            rows.append(html.Div(badges, className="mb-1"))
    return html.Div(rows)


def _build_field_row(entity_id: str, key: str, param: _Parameter, current_value: Any) -> dbc.Row:
    """Form row for a declared, schema-annotated parameter."""
    schema = param.to_schema()
    param_type = schema.get("type", "str")
    desc = schema.get("desc") or ""
    required = schema.get("required", False)
    choices = schema.get("choices")

    label_parts: List = [html.Code(key)]
    if required:
        label_parts.append(dbc.Badge("required", color="warning", className="ms-1", pill=True))
    if desc:
        label_parts.append(html.Small(f" — {desc}", className="text-muted ms-1"))

    input_comp = _build_input(
        entity_id,
        key,
        param_type,
        current_value,
        choices,
        is_bool=isinstance(param, BoolParameter),
    )
    return dbc.Row(
        [
            dbc.Label(label_parts, width=4, className="text-break"),
            dbc.Col(input_comp, width=8),
        ],
        className="mb-2 align-items-center",
    )


def _build_plain_field_row(entity_id: str, key: str, current_value: Any) -> dbc.Row:
    """Form row for an undeclared (unregistered) config key."""
    return dbc.Row(
        [
            dbc.Label(
                [html.Code(key), html.Small(" (unregistered)", className="text-danger ms-1")],
                width=4,
                className="text-break",
            ),
            dbc.Col(
                dbc.Input(
                    id={"type": f"{entity_id}-config-field", "key": key},
                    value=str(current_value) if current_value is not None else "",
                    type="text",
                    size="sm",
                    style={"backgroundColor": "rgba(220, 53, 69, 0.06)"},
                ),
                width=8,
            ),
        ],
        className="mb-2 align-items-center",
    )


def _build_input(
    entity_id: str,
    key: str,
    param_type: str,
    current_value: Any,
    choices: Optional[List],
    *,
    is_bool: bool,
) -> Any:
    """Return the appropriate Dash input component for *param_type*."""
    field_id = {"type": f"{entity_id}-config-field", "key": key}

    if choices:
        options = [{"label": c, "value": c} for c in choices]
        selected = str(current_value).lower() if current_value is not None else choices[0]
        return dbc.Select(id=field_id, options=options, value=selected, size="sm")

    if is_bool or param_type == "bool":
        # dbc.Checklist(switch=True) value is a list of selected option values
        return dbc.Checklist(
            id=field_id,
            options=[{"label": "", "value": "on"}],
            value=["on"] if current_value else [],
            switch=True,
            inline=True,
        )

    if param_type == "int":
        return dbc.Input(id=field_id, value=current_value, type="number", step=1, size="sm")

    if param_type == "float":
        return dbc.Input(id=field_id, value=current_value, type="number", step="any", size="sm")

    if param_type == "date":
        val = str(current_value)[:10] if current_value is not None else ""
        return dbc.Input(id=field_id, value=val, type="date", size="sm")

    if param_type == "list":
        val = ", ".join(str(v) for v in current_value) if isinstance(current_value, list) else str(current_value or "")
        return dbc.Textarea(id=field_id, value=val, size="sm", rows=2)

    # Default: plain text
    return dbc.Input(
        id=field_id,
        value=str(current_value) if current_value is not None else "",
        type="text",
        size="sm",
    )


def _build_entity_toggles(entities: List, entity_id: str, entity_type: str) -> html.Div:
    """Enable/disable checklist rows for sub-components or connectors."""
    if not entities:
        return html.Div(html.I(f"No {entity_type}s.", className="text-muted"))

    rows: List = []
    for entity in entities:
        key = entity.key
        name = entity.name
        cls_name = type(entity).__name__
        enabled = entity.configs.enabled if hasattr(entity, "configs") else True

        rows.append(
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Span(name, className="fw-semibold me-2"),
                            html.Small(cls_name, className="text-muted"),
                        ],
                        width=8,
                        className="d-flex align-items-center",
                    ),
                    dbc.Col(
                        dbc.Checklist(
                            id={"type": f"{entity_id}-{entity_type}-toggle", "key": key},
                            options=[{"label": "Enabled", "value": "enabled"}],
                            value=["enabled"] if enabled else [],
                            switch=True,
                            inline=True,
                        ),
                        width=4,
                        className="d-flex justify-content-end align-items-center",
                    ),
                ],
                className="mb-2",
            )
        )

    return html.Div(html.Div(rows))


def _register_callbacks(
    entity_id: str,
    configs,
    components: Optional[List],
    connectors: Optional[List],
) -> None:
    """Register open / save / discard callbacks for the modal.

    A single callback handles all three actions; ``ctx.triggered_id`` is used
    to distinguish them.
    """
    _id = entity_id

    @callback(
        Output(f"{_id}-config-modal", "is_open"),
        Output(f"{_id}-config-feedback", "children"),
        Input(f"{_id}-config-open-btn", "n_clicks"),
        Input(f"{_id}-config-save-btn", "n_clicks"),
        Input(f"{_id}-config-discard-btn", "n_clicks"),
        State(f"{_id}-config-modal", "is_open"),
        State({"type": f"{_id}-config-field", "key": ALL}, "value"),
        State({"type": f"{_id}-config-field", "key": ALL}, "id"),
        State({"type": f"{_id}-comp-toggle", "key": ALL}, "value"),
        State({"type": f"{_id}-comp-toggle", "key": ALL}, "id"),
        State({"type": f"{_id}-conn-toggle", "key": ALL}, "value"),
        State({"type": f"{_id}-conn-toggle", "key": ALL}, "id"),
        prevent_initial_call=True,
    )
    def _handle_modal(
        _open_clicks,
        _save_clicks,
        _discard_clicks,
        is_open,
        field_values,
        field_ids,
        comp_values,
        comp_ids,
        conn_values,
        conn_ids,
    ):
        triggered = ctx.triggered_id

        if triggered == f"{_id}-config-open-btn":
            return True, ""

        if triggered == f"{_id}-config-discard-btn":
            return False, ""

        if triggered == f"{_id}-config-save-btn":
            try:
                # Apply parameter field values
                for fid, value in zip(field_ids, field_values):
                    key = fid["key"]
                    # Checklist bool fields: value is a list (["on"] or [])
                    if isinstance(value, list):
                        value = bool(value)
                    # Skip empty strings — don't overwrite with nothing
                    if value is None or value == "":
                        continue
                    configs[key] = value
                configs.write()

                # Apply component enable/disable
                if components:
                    comp_map = {c.key: c for c in components}
                    for cid, value in zip(comp_ids, comp_values):
                        key = cid["key"]
                        if key in comp_map and hasattr(comp_map[key], "configs"):
                            comp_map[key].configs.enabled = bool(value)
                            comp_map[key].configs.write()

                # Apply connector enable/disable
                if connectors:
                    conn_map = {c.key: c for c in connectors}
                    for cid, value in zip(conn_ids, conn_values):
                        key = cid["key"]
                        if key in conn_map and hasattr(conn_map[key], "configs"):
                            conn_map[key].configs.enabled = bool(value)
                            conn_map[key].configs.write()

                return False, dbc.Alert(
                    "Configuration saved successfully.",
                    color="success",
                    duration=3000,
                    className="mb-0",
                )

            except Exception as exc:
                return True, dbc.Alert(
                    f"Error saving configuration: {exc}",
                    color="danger",
                    dismissable=True,
                    className="mb-0",
                )

        return is_open, ""
