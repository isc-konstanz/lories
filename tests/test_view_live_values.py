# -*- coding: utf-8 -*-
"""
tests.test_view_live_values
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Channel accordions on the component and connector pages are built once. Each update patches
only the items of channels that changed since the client's last update.
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


def _member(id_="sql"):
    return SimpleNamespace(id=id_, enabled=True)


def _channel(index, value=1.5, **kwargs):
    attributes = dict(
        id=f"system.component.channel_{index}",
        key=f"channel_{index}",
        name=f"Channel {index}",
        value=value,
        unit="W",
        type=float,
        freq="1min",
        state="valid",
        timestamp=pd.Timestamp("2026-09-30 12:00:00", tz="UTC"),
        connector=_member("modbus"),
        logger=_member(),
        is_valid=lambda: True,
        has_logger=lambda *_: False,
        get=lambda *_, **kw: kw.get("default"),
    )
    attributes.update(kwargs)
    return SimpleNamespace(**attributes)


class _Page:
    """A page's update callback behind the Dash server, called the way dash-renderer calls it."""

    def __init__(self, module: str, cls: str, build: str, accordion: str, channels) -> None:
        page = object.__new__(getattr(importlib.import_module(module), cls))
        page.id = f"live-{next(_pages)}"
        body = getattr(page, build)(channels)

        app = dash.Dash(__name__)
        app.layout = dash.html.Div([dash.dcc.Interval(id="view-update"), body])
        self._client = app.server.test_client()
        self._client.get("/_dash-layout")

        self.accordion = f"{page.id}-{accordion}"
        self.store = f"{self.accordion}-fingerprints"
        self._callback = next(key for key in app.callback_map if self.store in key)
        self.shown = None

    def update(self, *inputs):
        response = self._client.post(
            "/_dash-update-component",
            json={
                "output": self._callback,
                "outputs": [
                    {"id": self.accordion, "property": "children"},
                    {"id": self.store, "property": "data"},
                ],
                "inputs": [{"id": "view-update", "property": "n_intervals", "value": 1}, *inputs],
                "changedPropIds": ["view-update.n_intervals"],
                "state": [{"id": self.store, "property": "data", "value": self.shown}],
            },
        )
        if response.status_code == 204:
            return {}
        assert response.status_code == 200, response.data
        result = response.get_json()["response"]
        self.shown = result[self.store]["data"]
        patched = {}
        for operation in result[self.accordion]["children"]["operations"]:
            patched.setdefault(operation["location"][0], []).append(operation["params"]["value"])
        return patched


def _component_page(channels):
    return _Page("lories.application.view.pages.components.page", "ComponentPage", "_build_data", "data", channels)


def _connector_page(channels):
    return _Page(
        "lories.application.view.pages.connectors.page", "ConnectorPage", "_build_channels", "channels", channels
    )


@pytest.mark.parametrize("page", [_component_page, _connector_page])
def test_first_update_patches_every_item(page):
    assert sorted(page([_channel(i) for i in range(3)]).update()) == [0, 1, 2]


@pytest.mark.parametrize("page", [_component_page, _connector_page])
def test_unchanged_channels_are_not_resent(page):
    page = page([_channel(i) for i in range(3)])
    page.update()
    assert page.update() == {}


@pytest.mark.parametrize("page", [_component_page, _connector_page])
def test_only_changed_channels_are_resent(page):
    channels = [_channel(i) for i in range(3)]
    page = page(channels)
    page.update()

    channels[1].value = 42.0
    channels[1].timestamp = pd.Timestamp("2026-09-30 12:01:00", tz="UTC")
    updated = page.update()

    assert list(updated) == [1]
    assert "42.00" in json.dumps(updated[1])


@pytest.mark.parametrize("page", [_component_page, _connector_page])
def test_new_value_object_with_same_timestamp_is_resent(page):
    channels = [_channel(0)]
    page = page(channels)
    page.update()

    channels[0].value = 2.5
    assert list(page.update()) == [0]


@pytest.mark.parametrize("page", [_component_page, _connector_page])
def test_state_change_is_resent(page):
    channels = [_channel(0)]
    page = page(channels)
    page.update()

    channels[0].state = "disconnected"
    channels[0].is_valid = lambda: False
    assert "Disconnected" in json.dumps(page.update()[0])
