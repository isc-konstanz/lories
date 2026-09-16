# -*- coding: utf-8 -*-
"""
tests.test_view_channel_details
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The expanded channel accordion on the component page lists the channel id,
type, interval, connector and logger ahead of value and timestamp. Disabled
or missing connector/logger members render as a dash.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

html = pytest.importorskip("dash").html


def _member(id_="sql", enabled=True):
    return SimpleNamespace(id=id_, enabled=enabled)


def _channel(connector=None, logger=None, freq="15min"):
    return SimpleNamespace(
        id="pv.inverter.power",
        key="power",
        name="Power",
        value=4793.25,
        unit="W",
        type=float,
        freq=freq,
        state="valid",
        timestamp=None,
        connector=connector if connector is not None else _member("modbus"),
        logger=logger if logger is not None else _member(enabled=False),
        is_valid=lambda: True,
        has_logger=lambda *_: False,
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


def _rows(channel) -> dict:
    page_mod = importlib.import_module("lories.application.view.pages.components.page")
    page = object.__new__(page_mod.ComponentPage)
    page.id = "test"
    item = page._build_channel(channel)
    rows = {}
    for row in item.children:
        label_col, content_col = row.children
        rows[_text(label_col)] = content_col.children
    return rows


def test_detail_rows_order_and_content():
    rows = _rows(_channel())
    assert list(rows) == ["ID:", "Type:", "Interval:", "Connector:", "Logger:", "Value:", "Updated:", ""]
    assert _text(rows["ID:"]) == "pv.inverter.power"
    assert "font-monospace" in rows["ID:"].className
    assert _text(rows["Type:"]) == "float"
    assert _text(rows["Interval:"]) == "15min"
    assert _text(rows["Connector:"]) == "modbus"
    assert _text(rows["Logger:"]) == "—"


def test_detail_rows_missing_members():
    rows = _rows(_channel(connector=_member(id_=None), logger=_member("sql"), freq=None))
    assert _text(rows["Interval:"]) == "—"
    assert _text(rows["Connector:"]) == "—"
    assert _text(rows["Logger:"]) == "sql"
