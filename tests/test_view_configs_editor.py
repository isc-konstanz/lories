# -*- coding: utf-8 -*-
"""
tests.test_view_configs_editor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The config-editor modal ships as an empty shell inside the page layout; the
body (View + Edit tabs) is built server-side when the open button is clicked
and cleared again when the modal closes. Opening after a config change renders
the current values, not the values from layout-creation time.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("dash")
pytest.importorskip("dash_bootstrap_components")

import plotly.utils  # noqa: E402
from dash import no_update  # noqa: E402
from dash._callback import GLOBAL_CALLBACK_LIST, GLOBAL_CALLBACK_MAP  # noqa: E402
from dash._callback_context import context_value  # noqa: E402
from dash._utils import AttributeDict  # noqa: E402

from lories.application.view.pages.widgets.configs_editor import build_configs_editor_modal  # noqa: E402
from lories.core.configs.configurator import Configurator  # noqa: E402
from lories.core.configs.parameters import Parameter  # noqa: E402


class _Demo(Configurator):
    _port = Parameter(key="port", type=int, default=8080, desc="TCP port")
    _host = Parameter(key="host", type=str, required=True, desc="Host address")


def _json(component) -> str:
    return json.dumps(component, cls=plotly.utils.PlotlyJSONEncoder)


def _demo_configs(write_conf):
    return write_conf('port = 9000\nhost = "a.example"\nextra = "x"\n')


def _callback_entry(entity_id: str):
    key, entry = next((k, v) for k, v in GLOBAL_CALLBACK_MAP.items() if f"{entity_id}-config-modal.is_open" in k)
    return key, entry


def _trigger(entity_id: str, button: str):
    context_value.set(
        AttributeDict(triggered_inputs=[{"prop_id": f"{entity_id}-config-{button}.n_clicks", "value": 1}])
    )


def test_modal_shell_has_no_body(write_conf):
    configs = _demo_configs(write_conf)
    button, modal = build_configs_editor_modal(entity_id="ed-shell", configs=configs, configurator_type=_Demo)

    assert button.id == "ed-shell-config-open-btn"
    shell = _json(modal)
    assert "ed-shell-config-body" in shell
    assert "ed-shell-config-save-btn" in shell
    assert "Spinner" in shell
    assert "config-tab-view" not in shell
    assert "config-field" not in shell
    assert "9000" not in shell


def test_clientside_callback_opens_modal_instantly(write_conf):
    configs = _demo_configs(write_conf)
    build_configs_editor_modal(entity_id="ed-client", configs=configs, configurator_type=_Demo)

    spec = next(
        s for s in GLOBAL_CALLBACK_LIST if s.get("clientside_function") and "ed-client-config-modal" in s["output"]
    )
    assert "is_open" in spec["output"]
    assert spec["inputs"] == [{"id": "ed-client-config-open-btn", "property": "n_clicks"}]


def test_modal_callback_outputs_body(write_conf):
    configs = _demo_configs(write_conf)
    build_configs_editor_modal(entity_id="ed-wire", configs=configs, configurator_type=_Demo)

    key, _ = _callback_entry("ed-wire")
    assert "ed-wire-config-body.children" in key
    assert "ed-wire-config-feedback.children" in key


def test_open_builds_body_with_current_values(write_conf):
    configs = _demo_configs(write_conf)
    build_configs_editor_modal(entity_id="ed-open", configs=configs, configurator_type=_Demo)
    handler = _callback_entry("ed-open")[1]["callback"].__wrapped__

    _trigger("ed-open", "open-btn")
    is_open, feedback, body = handler(1, None, None, False, [], [], [], [], [], [])
    assert is_open is no_update
    rendered = _json(body)
    assert "ed-open-config-tab-view" in rendered
    assert "ed-open-config-tab-edit" in rendered
    assert "9000" in rendered

    configs["port"] = 9100
    _trigger("ed-open", "open-btn")
    rendered = _json(handler(2, None, None, False, [], [], [], [], [], [])[2])
    assert "9100" in rendered
    assert "9000" not in rendered


def test_discard_closes_and_resets_body(write_conf):
    configs = _demo_configs(write_conf)
    build_configs_editor_modal(entity_id="ed-drop", configs=configs, configurator_type=_Demo)
    handler = _callback_entry("ed-drop")[1]["callback"].__wrapped__

    _trigger("ed-drop", "discard-btn")
    is_open, feedback, body = handler(None, None, 1, True, [], [], [], [], [], [])
    assert is_open is False
    assert "Spinner" in _json(body)
    assert "config-tab-view" not in _json(body)


def test_save_writes_values_and_resets_body(write_conf, tmp_path):
    configs = _demo_configs(write_conf)
    build_configs_editor_modal(entity_id="ed-save", configs=configs, configurator_type=_Demo)
    handler = _callback_entry("ed-save")[1]["callback"].__wrapped__

    field_ids = [{"type": "ed-save-config-field", "key": "port"}]
    _trigger("ed-save", "save-btn")
    is_open, feedback, body = handler(None, 1, None, True, [9100], field_ids, [], [], [], [])
    assert is_open is False
    assert "Spinner" in _json(body)
    assert configs["port"] == 9100
    assert "9100" in (tmp_path / "test.conf").read_text()
