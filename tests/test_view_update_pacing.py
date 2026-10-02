# -*- coding: utf-8 -*-
"""
tests.test_view_update_pacing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Page updates pass through a request/echo gate; the tick interval is the ``update_interval`` setting.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

import pandas as pd

dash = pytest.importorskip("dash")

from lories.application.view.interface import ViewInterface  # noqa: E402
from lories.core.configs.errors import ConfigurationError  # noqa: E402


def _channel():
    return SimpleNamespace(
        id="system.component.power",
        key="power",
        name="Power",
        value=1.0,
        unit="W",
        type=float,
        freq=None,
        state="valid",
        timestamp=pd.NaT,
        connector=None,
        logger=None,
        is_valid=lambda: True,
        has_logger=lambda *_: False,
        get=lambda *_, **kw: kw.get("default"),
    )


@pytest.mark.parametrize(
    "module, cls, build, section",
    [
        ("lories.application.view.pages.components.page", "ComponentPage", "_build_data", "data"),
        ("lories.application.view.pages.connectors.page", "ConnectorPage", "_build_channels", "channels"),
    ],
)
def test_updates_pass_through_the_gate(module, cls, build, section):
    page = object.__new__(getattr(importlib.import_module(module), cls))
    page.id = f"gate-{cls.lower()}"
    body = getattr(page, build)([_channel()])

    app = dash.Dash(__name__)
    app.layout = dash.html.Div([dash.dcc.Interval(id="view-update"), body])
    dependencies = app.server.test_client().get("/_dash-dependencies").get_json()
    requested = f"{page.id}-{section}-requested"

    (update,) = [d for d in dependencies if f"{page.id}-{section}-received.data" in d["output"]]
    assert [(i["id"], i["property"]) for i in update["inputs"]] == [(requested, "data")]
    (gate,) = [d for d in dependencies if d["output"] == f"{requested}.data"]
    assert gate["clientside_function"] is not None
    assert ("view-update", "n_intervals") in [(i["id"], i["property"]) for i in gate["inputs"]]


def test_update_interval_setting(write_conf):
    parameter = ViewInterface._update_interval
    assert parameter.resolve(write_conf("")) == pd.Timedelta("1s")
    assert parameter.resolve(write_conf('update_interval = "5s"\n')) == pd.Timedelta("5s")
    with pytest.raises(ConfigurationError):
        parameter.resolve(write_conf('update_interval = "500ms"\n'))


def test_layout_ticks_at_the_update_interval():
    interface = object.__new__(ViewInterface)
    interface._update_interval = pd.Timedelta("5s")
    interface._Interface__context = SimpleNamespace(id="app")
    interface.view = SimpleNamespace(header=SimpleNamespace(navbar=dash.html.Div()))

    layout = interface.create_layout()
    intervals = {c.id: c.interval for c in layout.children if isinstance(c, dash.dcc.Interval)}
    assert intervals == {"view-update": 5000}
