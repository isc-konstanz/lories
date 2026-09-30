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

    def __init__(self, module: str, cls: str, build: str, accordion: str, channels, opens=False) -> None:
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
        self._opens = opens
        self.shown = None

    def update(self, active=None):
        """Patched items by index, each as ``{patched prop or "item": value}``."""
        inputs = [{"id": "view-update", "property": "n_intervals", "value": 1}]
        if self._opens:
            inputs.append({"id": self.accordion, "property": "active_item", "value": active})
        response = self._client.post(
            "/_dash-update-component",
            json={
                "output": self._callback,
                "outputs": [
                    {"id": self.accordion, "property": "children"},
                    {"id": self.store, "property": "data"},
                ],
                "inputs": inputs,
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
            index, *prop = operation["location"]
            patched.setdefault(index, {})[prop[-1] if prop else "item"] = operation["params"]["value"]
        return patched


def _component_page(channels):
    return _Page(
        "lories.application.view.pages.components.page", "ComponentPage", "_build_data", "data", channels, opens=True
    )


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


def test_details_are_built_only_for_open_items():
    channels = [_channel(i, has_logger=lambda *_: True) for i in range(2)]
    page = _component_page(channels)
    assert all(set(item) == {"title"} for item in page.update().values())

    opened = page.update(active=[channels[0].id])
    assert list(opened) == [0] and set(opened[0]) == {"children"}
    assert "Placeholder" in json.dumps(opened[0]["children"])
    assert page.update(active=[channels[0].id]) == {}


def test_open_details_follow_channel_changes():
    channels = [_channel(0, has_logger=lambda *_: True)]
    page = _component_page(channels)
    page.update(active=[channels[0].id])

    channels[0].value = 3.5
    assert set(page.update(active=[channels[0].id])[0]) == {"title", "children"}


def test_closed_details_are_rebuilt_on_reopen_only_after_a_change():
    channels = [_channel(0, has_logger=lambda *_: True)]
    page = _component_page(channels)
    page.update(active=[channels[0].id])

    channels[0].value = 3.5
    assert set(page.update(active=[])[0]) == {"title"}
    assert set(page.update(active=[channels[0].id])[0]) == {"children"}
    assert page.update(active=[]) == {}
    assert page.update(active=[channels[0].id]) == {}


def test_image_details_link_the_image_route():
    channel = _channel(0, value=b"\x89PNG\r\n", type=bytes, unit="png")
    page = _component_page([channel])
    page.update()

    details = json.dumps(page.update(active=channel.id)[0]["children"])
    assert f"/api/image/{channel.id}?v=" in details
    assert "base64" not in details


def test_binary_details_show_the_size_only():
    channel = _channel(0, value=b"\x00" * 2048, type=bytes, unit="-")
    page = _component_page([channel])
    assert "(2,048 bytes)" in json.dumps(page.update()[0]["title"])

    details = json.dumps(page.update(active=[channel.id])[0]["children"])
    assert "data:image" not in details and "/api/image" not in details
