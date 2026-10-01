# -*- coding: utf-8 -*-
"""
tests.test_view_list_updates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Component and connector lists answer no_update while their statuses are unchanged.
"""

from __future__ import annotations

import importlib
import itertools
import json
from types import SimpleNamespace

import pytest

import pandas as pd

dash = pytest.importorskip("dash")

_pages = itertools.count()


def _component(index):
    component = SimpleNamespace(
        id=f"system.component_{index}",
        key=f"component_{index}",
        name=f"Component {index}",
        enabled=True,
        active=True,
    )
    component.is_enabled = lambda: component.enabled
    component.is_active = lambda: component.active
    return component


def _connector(index):
    connector = SimpleNamespace(
        id=f"connector_{index}",
        key=f"connector_{index}",
        name=f"Connector {index}",
        enabled=True,
        _connected=True,
        _timestamp_connect=pd.Timestamp("2026-09-30 12:00:00", tz="UTC"),
        _timestamp_disconnect=pd.NaT,
    )
    connector.is_enabled = lambda: connector.enabled
    return connector


class _ListPage:
    """A list's update callback behind the Dash server, called the way dash-renderer calls it."""

    def __init__(self, build: str, accordion: str, entities) -> None:
        module = importlib.import_module("lories.application.view.pages.components.page")
        page = object.__new__(module.ComponentPage)
        page.id = f"list-{next(_pages)}"
        page.key = page.id
        page.group = None
        body = getattr(page, build)(entities)

        app = dash.Dash(__name__)
        app.layout = dash.html.Div([dash.dcc.Interval(id="view-update"), body])
        self._client = app.server.test_client()
        self._client.get("/_dash-layout")

        self.accordion = f"{page.id}-{accordion}"
        self.store = f"{self.accordion}-fingerprints"
        self.callback = next(key for key in app.callback_map if self.store in key)
        self.shown = None
        self.tick = 0

    def update(self):
        """The accordion's fresh items, or ``None`` when the callback answered no_update."""
        self.tick += 1
        response = self._client.post(
            "/_dash-update-component",
            json={
                "output": self.callback,
                "outputs": [
                    {"id": self.accordion, "property": "children"},
                    {"id": self.store, "property": "data"},
                ],
                "inputs": [{"id": "view-update", "property": "n_intervals", "value": self.tick}],
                "changedPropIds": ["view-update.n_intervals"],
                "state": [{"id": self.store, "property": "data", "value": self.shown}],
            },
        )
        assert response.status_code == 200, response.data
        result = response.get_json()["response"]
        if self.store not in result:
            assert result == {}
            return None
        self.shown = result[self.store]["data"]
        return result[self.accordion]["children"]


def _component_list(components):
    return _ListPage("_build_components", "components", components)


def _connector_list(connectors):
    return _ListPage("_build_connectors", "connectors", connectors)


@pytest.mark.parametrize("page, entity", [(_component_list, _component), (_connector_list, _connector)])
def test_unchanged_lists_answer_no_update(page, entity):
    page = page([entity(i) for i in range(3)])
    assert len(page.update()) == 3
    assert page.update() is None
    assert page.update() is None


def test_component_status_flip_redraws():
    components = [_component(i) for i in range(2)]
    page = _component_list(components)
    items = page.update()
    assert "Active" in json.dumps(items[1])
    assert "component-1" in json.dumps(items[1])

    components[1].active = False
    assert "Inactive" in json.dumps(page.update()[1])
    assert page.update() is None

    components[1].enabled = False
    assert "Disabled" in json.dumps(page.update()[1])
    assert page.update() is None


def test_connector_status_flip_redraws():
    connectors = [_connector(0)]
    page = _connector_list(connectors)
    items = page.update()
    assert "Connected" in json.dumps(items[0])
    assert "2026-09-30 12:00:00" in json.dumps(items[0])
    assert "/connector/connector-0" in json.dumps(items[0])

    connectors[0]._connected = False
    connectors[0]._timestamp_connect = pd.NaT
    connectors[0]._timestamp_disconnect = pd.Timestamp("2026-09-30 12:05:00", tz="UTC")
    items = page.update()
    assert "Disconnected" in json.dumps(items[0])
    assert "2026-09-30 12:05:00" in json.dumps(items[0])
    assert page.update() is None


def test_connector_timestamp_move_redraws():
    connectors = [_connector(0)]
    page = _connector_list(connectors)
    page.update()

    connectors[0]._timestamp_connect = pd.Timestamp("2026-09-30 12:10:00", tz="UTC")
    assert "2026-09-30 12:10:00" in json.dumps(page.update()[0])
    assert page.update() is None


@pytest.mark.parametrize("page, entity", [(_component_list, _component), (_connector_list, _connector)])
def test_page_reload_redraws_unchanged_lists(page, entity):
    page = page([entity(i) for i in range(2)])
    page.update()
    assert page.update() is None

    page.shown = None
    assert len(page.update()) == 2
    assert page.update() is None
