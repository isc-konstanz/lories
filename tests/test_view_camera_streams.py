# -*- coding: utf-8 -*-
"""
tests.test_view_camera_streams
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Only the camera page's viewer embeds MJPEG streams; the overview card and channel details show stills.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import pandas as pd

pytest.importorskip("dash")

from dash._utils import to_json  # noqa: E402

from lories.application.view.pages import PageLayout  # noqa: E402
from lories.application.view.pages.components.camera import CameraPage  # noqa: E402
from lories.application.view.pages.components.page import ComponentPage  # noqa: E402
from lories.components.cameras._core import _Camera  # noqa: E402


def _json(component) -> str:
    return json.dumps(json.loads(to_json(component)))


class _Data(dict):
    def __contains__(self, key):
        return super().__contains__(key) or any(channel.id == key for channel in self.values())


def _channel(key, stream):
    return SimpleNamespace(
        id=f"system.camera.{key}",
        key=key,
        value=b"\xff\xd8",
        unit=None,
        type=bytes,
        state="valid",
        timestamp=pd.Timestamp("2026-09-30 12:00:00", tz="UTC"),
        is_valid=lambda: True,
        get=lambda key, default=None: stream if key == "stream" else default,
    )


def _camera_page(monkeypatch, name):
    monkeypatch.setattr(ComponentPage, "create_layout", lambda self, layout: None)
    data = _Data(
        {
            _Camera.FRAME: _channel("frame", False),
            _Camera.STREAM: _channel("stream", True),
            _Camera.MOTION: _channel("motion", True),
        }
    )
    page = object.__new__(CameraPage)
    page.id = name
    page._component = SimpleNamespace(id=f"system.{name}", preview=True, data=data, has_protection=lambda: False)
    return page


def test_overview_card_shows_the_still_frame_only(monkeypatch):
    page = _camera_page(monkeypatch, "camera-card")
    layout = PageLayout(id="camera-card-container")
    page.create_layout(layout)

    card = _json(layout.card)
    assert "/api/snapshot/" in card and "/api/stream/" not in card
    assert _json(layout.container).count("/api/stream/") == 2


def test_camera_details_show_stills_of_streams(monkeypatch):
    page = _camera_page(monkeypatch, "camera-details")
    body = _json(page._build_channel_body(page._component.data[_Camera.STREAM]))
    assert "/api/image/system.camera.stream?v=" in body and "/api/stream/" not in body


def test_component_details_show_stills_of_streams():
    page = object.__new__(ComponentPage)
    page.id = "component-details"
    body = _json(page._build_channel_body(_channel("feed", True)))
    assert "/api/image/system.camera.feed?v=" in body and "/api/stream/" not in body
