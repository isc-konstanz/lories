# -*- coding: utf-8 -*-
"""
tests.test_view_image_route
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``/api/image/<channel_id>`` serves the current value of a bytes channel as one image.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

flask = pytest.importorskip("flask")

from lories.application.view.stream import register_stream_routes  # noqa: E402


def _client(*channels):
    server = flask.Flask(__name__)
    register_stream_routes(server, {channel.id: channel for channel in channels}.get)
    return server.test_client()


def _channel(value, unit=None, type_=bytes):
    return SimpleNamespace(id="system.camera.frame", value=value, unit=unit, type=type_)


def test_image_is_served_with_its_unit_type():
    response = _client(_channel(b"\x89PNG", unit="png")).get("/api/image/system.camera.frame")
    assert response.status_code == 200
    assert response.mimetype == "image/png"
    assert response.data == b"\x89PNG"


def test_image_without_unit_is_served_as_jpeg():
    response = _client(_channel(b"\xff\xd8")).get("/api/image/system.camera.frame")
    assert response.mimetype == "image/jpeg"


@pytest.mark.parametrize(
    "channels, status",
    [
        ((), 404),
        ((_channel(1.0, type_=float),), 400),
        ((_channel(None),), 503),
    ],
)
def test_image_errors(channels, status):
    assert _client(*channels).get("/api/image/system.camera.frame").status_code == status
