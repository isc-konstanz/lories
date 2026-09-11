# -*- coding: utf-8 -*-
"""
tests.test_view_channel_header
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Channel accordion headers on the component and connector pages render the
value, unit and state as three fixed-width columns: value right-aligned,
unit left-aligned, state left-aligned. Invalid channels keep empty value
and unit cells so the state column stays aligned.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from lories.application.view._dash_format import HEADER_STATE_STYLE, HEADER_UNIT_STYLE, HEADER_VALUE_STYLE

html = pytest.importorskip("dash").html


def _channel(value=4793.25, unit="W", valid=True, state="valid", type_=float):
    return SimpleNamespace(
        key="power",
        name="Power",
        value=value,
        unit=unit,
        type=type_,
        state=state,
        timestamp=None,
        is_valid=lambda: valid,
        get=lambda *_, **kw: kw.get("default"),
    )


def _text(node) -> str:
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    children = getattr(node, "children", None)
    if isinstance(children, (list, tuple)):
        return "".join(_text(c) for c in children)
    return _text(children)


def _component_header(channel):
    page_mod = importlib.import_module("lories.application.view.pages.components.page")
    page = object.__new__(page_mod.ComponentPage)
    return page._build_channel_header(channel)


def _connector_header(channel):
    page_mod = importlib.import_module("lories.application.view.pages.connectors.page")
    page = object.__new__(page_mod.ConnectorPage)
    page.id = "test"
    item = page._build_channel(channel)
    return item.title.children[1].children


@pytest.mark.parametrize("build", [_component_header, _connector_header])
def test_header_columns(build):
    header = build(_channel())
    assert isinstance(header, html.Div)
    assert "d-flex" in header.className
    value_col, unit_col, state_col = header.children
    assert value_col.style == HEADER_VALUE_STYLE
    assert unit_col.style == HEADER_UNIT_STYLE
    assert state_col.style == HEADER_STATE_STYLE
    assert HEADER_VALUE_STYLE["textAlign"] == "right"
    assert HEADER_UNIT_STYLE["textAlign"] == "left"
    assert _text(value_col) == "4793.25"
    assert _text(unit_col) == "W"
    assert _text(state_col) == "Valid"


@pytest.mark.parametrize("build", [_component_header, _connector_header])
def test_header_invalid_keeps_columns(build):
    header = build(_channel(value=None, valid=False, state="disconnected"))
    value_col, unit_col, state_col = header.children
    assert value_col.children is None
    assert unit_col.children is None
    assert _text(state_col) == "Disconnected"
    assert "text-warning" in state_col.children.className


def test_component_header_zero_and_scientific():
    assert _text(_component_header(_channel(value=0.0)).children[0]) == "0.00"
    assert _text(_component_header(_channel(value=12345678.0)).children[0]) == "1.23e+07"
