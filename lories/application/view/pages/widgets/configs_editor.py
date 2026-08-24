# -*- coding: utf-8 -*-
"""
lories.application.view.pages.widgets.configs_editor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Config editor modal for Dash pages.

Renders a per-entity "Edit Configs" button that opens a ``dbc.Modal`` with:
  - Editable input fields for every declared ``_Parameter``
  - Enable/disable toggles for sub-components and connectors
  - Per-row remove button to delete a sub-component / connector
  - Inline "Add" form (type select + key + name) per section
  - Save  → writes parameter changes and enable toggles to disk
  - Discard → closes without applying parameter changes

Add and Remove are immediate actions: they call the live ``RegistratorAccess``
APIs of the parent and persist to disk independently of the Save button.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Tuple

import dash_bootstrap_components as dbc
from dash import ALL, Input, Output, State, callback, ctx, html, no_update

from lories.application.view.pages.widgets.configs import build_configs_widget
from lories.core.configs.parameters import (
    BoolParameter,
    ParameterGroup,
    _EntityParameter,
    _Parameter,
)

_logger = logging.getLogger(__name__)


def build_configs_editor_modal(
    entity_id: str,
    configs,
    configurator_type: type,
    *,
    components: Optional[List] = None,
    connectors: Optional[List] = None,
    components_access=None,
    connectors_access=None,
) -> Tuple[dbc.Button, dbc.Modal]:
    """Return *(open_button, modal)* for editing *configs* in a dialog.

    When ``components_access`` / ``connectors_access`` (a live
    ``RegistratorAccess``) is supplied, the matching section gains an inline
    "Add" form and per-row Remove buttons. Without it, only enable/disable
    toggles are shown.
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
                        components_access,
                        connectors_access,
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

    _register_callbacks(
        entity_id,
        configs,
        components,
        connectors,
        components_access,
        connectors_access,
    )

    return open_button, modal


def _build_modal_body(
    configs,
    configurator_type: type,
    entity_id: str,
    components: Optional[List],
    connectors: Optional[List],
    components_access,
    connectors_access,
) -> html.Div:
    edit_sections: List[Any] = []

    params: Dict[str, _Parameter] = getattr(configurator_type, "__config_parameters__", {})
    edit_sections.append(_build_param_fields(configs, params, entity_id))

    if components is not None or components_access is not None:
        edit_sections.append(
            _build_entity_section(
                entity_id=entity_id,
                entity_type="comp",
                title="Components",
                entities=components if components is not None else list(components_access.values()),
                access=components_access,
                params=params,
                slot_kind="component",
            )
        )

    if connectors is not None or connectors_access is not None:
        edit_sections.append(
            _build_entity_section(
                entity_id=entity_id,
                entity_type="conn",
                title="Connectors",
                entities=connectors if connectors is not None else list(connectors_access.values()),
                access=connectors_access,
                params=params,
                slot_kind="connector",
            )
        )

    view_widget = build_configs_widget(
        configs,
        configurator_type,
        prefix=f"{entity_id}-view-",
    )

    return html.Div(
        dbc.Tabs(
            [
                dbc.Tab(
                    html.Div(view_widget, className="pt-3"),
                    label="View",
                    tab_id=f"{entity_id}-config-tab-view",
                ),
                dbc.Tab(
                    html.Div(edit_sections, className="pt-3"),
                    label="Edit",
                    tab_id=f"{entity_id}-config-tab-edit",
                ),
            ],
            active_tab=f"{entity_id}-config-tab-view",
            id=f"{entity_id}-config-tabs",
        )
    )


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

    for key, param in param_by_key.items():
        current_value = configs.get(key) if (configs and key in configs) else None
        if current_value is None and hasattr(param, "default"):
            current_value = param.default
        rows.append(_build_field_row(entity_id, key, param, current_value))

    if configs:
        skip_keys = set(param_by_key) | group_keys | set(entity_params)
        for key, value in configs.items():
            if key not in skip_keys and not isinstance(value, Mapping):
                rows.append(_build_plain_field_row(entity_id, key, value))

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

    if param_type == "timedelta":
        return dbc.Input(
            id=field_id,
            value=str(current_value) if current_value is not None else "",
            type="text",
            size="sm",
            placeholder="e.g. 30s, 5min, 1h, 1D",
        )

    if param_type == "list":
        val = ", ".join(str(v) for v in current_value) if isinstance(current_value, list) else str(current_value or "")
        return dbc.Textarea(id=field_id, value=val, size="sm", rows=2)

    return dbc.Input(
        id=field_id,
        value=str(current_value) if current_value is not None else "",
        type="text",
        size="sm",
    )


def _build_entity_section(
    entity_id: str,
    entity_type: str,
    title: str,
    entities: List,
    access,
    params: Dict[str, _Parameter],
    slot_kind: str,
) -> dbc.Accordion:
    """Build an accordion section: existing rows + Add form."""
    type_options = _collect_type_options(access, params, slot_kind)

    children: List[Any] = [
        html.Div(
            _build_entity_rows(entities, entity_id, entity_type, can_remove=access is not None),
            id=f"{entity_id}-{entity_type}-list",
        ),
    ]

    if access is not None:
        children.append(html.Hr(className="mt-3 mb-2"))
        children.append(
            html.Div(
                _build_add_form(entity_id, entity_type, type_options),
                id=f"{entity_id}-{entity_type}-add-form",
            )
        )

    return dbc.Accordion(
        dbc.AccordionItem(
            title=title,
            children=children,
            item_id=f"{entity_id}-editor-{entity_type}s",
        ),
        active_item=f"{entity_id}-editor-{entity_type}s",
        always_open=True,
        className="mb-2",
    )


def _collect_type_options(access, params: Dict[str, _Parameter], slot_kind: str) -> List[Dict[str, str]]:
    """Return only types declared via ``ComponentParameter`` / ``ConnectorParameter`` slots.

    Iterates the configurator's ``__config_parameters__`` for ``_EntityParameter``
    descriptors of the matching ``slot_kind`` and collects their ``fitting_types``
    schema. Types not claimed by any declared entity slot are not offered.
    """
    fitting: Dict[str, Dict[str, str]] = {}
    for p in params.values():
        if not isinstance(p, _EntityParameter):
            continue
        schema = p.to_schema()
        if schema.get("type") != slot_kind:
            continue
        for entry in schema.get("fitting_types") or []:
            if not entry.get("available", True):
                continue
            key = entry["key"]
            if key in fitting:
                continue
            cls_name = entry.get("cls")
            label = f"{key} ({cls_name})" if cls_name else key
            fitting[key] = {"label": label, "value": key}

    return [fitting[k] for k in sorted(fitting)]


def _build_add_form(entity_id: str, entity_type: str, type_options: List[Dict[str, str]]) -> List[Any]:
    """Inline form: Type select + Key input + Name input + Add button."""
    initial_value = type_options[0]["value"] if type_options else None
    slot_kind = "component" if entity_type == "comp" else "connector"
    placeholder_msg = f"No declared {slot_kind} slots" if not type_options else "Select type…"

    return [
        html.Small(f"Add new {entity_type}:", className="text-muted d-block mb-1"),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Select(
                        id=f"{entity_id}-{entity_type}-add-type",
                        options=type_options,
                        value=initial_value,
                        placeholder=placeholder_msg,
                        size="sm",
                    ),
                    width=4,
                ),
                dbc.Col(
                    dbc.Input(
                        id=f"{entity_id}-{entity_type}-add-key",
                        type="text",
                        placeholder="key (e.g. battery_1)",
                        size="sm",
                    ),
                    width=3,
                ),
                dbc.Col(
                    dbc.Input(
                        id=f"{entity_id}-{entity_type}-add-name",
                        type="text",
                        placeholder="name (optional)",
                        size="sm",
                    ),
                    width=3,
                ),
                dbc.Col(
                    dbc.Button(
                        "Add",
                        id=f"{entity_id}-{entity_type}-add-btn",
                        color="success",
                        size="sm",
                        outline=True,
                        disabled=not type_options,
                    ),
                    width=2,
                    className="d-grid",
                ),
            ],
            className="g-2 align-items-center",
        ),
        html.Div(id=f"{entity_id}-{entity_type}-add-feedback", className="mt-2"),
    ]


def _build_entity_rows(
    entities: List,
    entity_id: str,
    entity_type: str,
    can_remove: bool,
) -> html.Div:
    """Per-row layout: name + class + enable toggle + (optional) remove button."""
    if not entities:
        return html.Div(html.I(f"No {entity_type}s.", className="text-muted"))

    rows: List = []
    for entity in entities:
        key = entity.key
        name = getattr(entity, "name", key)
        cls_name = type(entity).__name__
        enabled = entity.configs.enabled if hasattr(entity, "configs") else True

        controls: List[Any] = [
            dbc.Checklist(
                id={"type": f"{entity_id}-{entity_type}-toggle", "key": key},
                options=[{"label": "Enabled", "value": "enabled"}],
                value=["enabled"] if enabled else [],
                switch=True,
                inline=True,
            ),
        ]
        if can_remove:
            controls.append(
                dbc.Button(
                    "Remove",
                    id={"type": f"{entity_id}-{entity_type}-remove", "key": key},
                    color="danger",
                    outline=True,
                    size="sm",
                    className="ms-2",
                )
            )

        rows.append(
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Span(name, className="fw-semibold me-2"),
                            html.Small(cls_name, className="text-muted"),
                        ],
                        width=6,
                        className="d-flex align-items-center",
                    ),
                    dbc.Col(
                        controls,
                        width=6,
                        className="d-flex justify-content-end align-items-center",
                    ),
                ],
                className="mb-2",
            )
        )

    return html.Div(rows)


def _add_child(access, key: str, type_str: str, name: Optional[str]) -> None:
    """Add a child via RegistratorAccess.add(...) and persist parent configs."""
    add_kwargs: Dict[str, Any] = {"type": type_str}
    if name:
        add_kwargs["name"] = name
    access.add(key, **add_kwargs)
    try:
        access._registrar.configs.write()
    except Exception as exc:
        _logger.debug("Persisting parent configs after add failed: %s", exc)


def _remove_child(access, key: str) -> None:
    """Remove a child: drop from access, pop config member, delete .conf file."""
    target = None
    for entity in list(access.values()):
        if entity.key == key:
            target = entity
            break
    if target is None:
        raise KeyError(f"No child with key {key!r}")

    access._remove(target)

    try:
        registrators_configs = access._load_registrators_configs()
        if registrators_configs.has_member(key):
            registrators_configs.pop_member(key)
    except Exception as exc:
        _logger.debug("Popping config member %r failed: %s", key, exc)

    try:
        config_path = target.configs.path
        parent_path = access._registrar.configs.path
        if config_path and config_path != parent_path and os.path.isfile(config_path):
            os.remove(config_path)
    except Exception as exc:
        _logger.debug("Deleting child config file failed: %s", exc)

    try:
        access._registrar.configs.write()
    except Exception as exc:
        _logger.debug("Persisting parent configs after remove failed: %s", exc)


def _register_callbacks(
    entity_id: str,
    configs,
    components: Optional[List],
    connectors: Optional[List],
    components_access,
    connectors_access,
) -> None:
    """Register open / save / discard + add / remove callbacks for the modal."""
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
                for fid, value in zip(field_ids, field_values):
                    key = fid["key"]
                    if isinstance(value, list):
                        value = bool(value)
                    if value is None or value == "":
                        continue
                    configs[key] = value
                configs.write()

                _apply_toggles(comp_ids, comp_values, components_access, components)
                _apply_toggles(conn_ids, conn_values, connectors_access, connectors)

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

    if components_access is not None:
        _register_entity_callbacks(_id, "comp", components_access, "component")
    if connectors_access is not None:
        _register_entity_callbacks(_id, "conn", connectors_access, "connector")


def _apply_toggles(ids: List[Dict[str, str]], values: List[List[str]], access, fallback_list) -> None:
    """Apply enable/disable toggles for sub-entities."""
    if not ids:
        return
    if access is not None:
        entity_map = {e.key: e for e in access.values()}
    elif fallback_list:
        entity_map = {e.key: e for e in fallback_list}
    else:
        return

    for cid, value in zip(ids, values):
        key = cid["key"]
        entity = entity_map.get(key)
        if entity is None or not hasattr(entity, "configs"):
            continue
        entity.configs.enabled = bool(value)
        entity.configs.write()


def _register_entity_callbacks(entity_id: str, entity_type: str, access, slot_kind: str) -> None:
    """Add/Remove callbacks for one entity section."""

    @callback(
        Output(f"{entity_id}-{entity_type}-list", "children"),
        Output(f"{entity_id}-{entity_type}-add-feedback", "children"),
        Output(f"{entity_id}-{entity_type}-add-key", "value"),
        Output(f"{entity_id}-{entity_type}-add-name", "value"),
        Input(f"{entity_id}-{entity_type}-add-btn", "n_clicks"),
        Input({"type": f"{entity_id}-{entity_type}-remove", "key": ALL}, "n_clicks"),
        State(f"{entity_id}-{entity_type}-add-type", "value"),
        State(f"{entity_id}-{entity_type}-add-key", "value"),
        State(f"{entity_id}-{entity_type}-add-name", "value"),
        prevent_initial_call=True,
    )
    def _handle_entity(_add_clicks, remove_clicks, add_type, add_key, add_name):
        triggered = ctx.triggered_id

        if triggered == f"{entity_id}-{entity_type}-add-btn":
            if not add_type:
                return no_update, _alert("Type is required.", "warning"), no_update, no_update
            if not add_key:
                return no_update, _alert("Key is required.", "warning"), no_update, no_update

            if any(e.key == add_key for e in access.values()):
                return no_update, _alert(f"Key '{add_key}' already exists.", "warning"), no_update, no_update

            try:
                _add_child(access, add_key, add_type, add_name)
            except Exception as exc:
                return no_update, _alert(f"Failed to add: {exc}", "danger"), no_update, no_update

            rows = _build_entity_rows(list(access.values()), entity_id, entity_type, can_remove=True)
            return rows, _alert(f"Added {slot_kind} '{add_key}'.", "success"), "", ""

        if isinstance(triggered, dict) and triggered.get("type") == f"{entity_id}-{entity_type}-remove":
            if not any(remove_clicks):
                return no_update, no_update, no_update, no_update
            key = triggered.get("key")
            try:
                _remove_child(access, key)
            except Exception as exc:
                return no_update, _alert(f"Failed to remove: {exc}", "danger"), no_update, no_update

            rows = _build_entity_rows(list(access.values()), entity_id, entity_type, can_remove=True)
            return rows, _alert(f"Removed {slot_kind} '{key}'.", "success"), no_update, no_update

        return no_update, no_update, no_update, no_update


def _alert(msg: str, color: str) -> dbc.Alert:
    return dbc.Alert(msg, color=color, duration=3000, dismissable=True, className="mb-0")
