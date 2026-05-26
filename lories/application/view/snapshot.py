# -*- coding: utf-8 -*-
"""
lories.application.view.snapshot
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Single-frame JPEG snapshots for camera components.

A Flask route at ``/api/snapshot/<component_id>`` returns the latest still
frame as ``image/jpeg``. The response is the channel's most recently cached
value (refreshed by the application's read loop at the channel's ``freq``).

We deliberately *don't* call ``CameraConnector.read_frame()`` here: for
non-streaming captures it opens, drains, reads, and tears down an RTSP
connection per call — too heavy for browser-paced polling and races the
main read loop on the same underlying capture.
"""

from __future__ import annotations

from typing import Callable, Optional

from flask import Flask, Response, abort

from lories.components.cameras._core import _Camera


def register_snapshot_routes(
    server: Flask,
    find_component: Callable[[str], Optional[object]],
    *,
    require_login: bool = False,
) -> None:
    """Register the snapshot route on *server*.

    Parameters
    ----------
    server:
        The Flask app underlying the Dash server (``Dash.server``).
    find_component:
        Callable resolving a fully-qualified component id to a ``Component``
        instance (or ``None`` if unknown).
    require_login:
        When ``True``, reject requests from unauthenticated users with 401.
    """

    @server.route("/api/snapshot/<path:component_id>")
    def snapshot_route(component_id: str):  # noqa: WPS430 — Flask requires a named view
        if require_login:
            from flask_login import current_user

            if not getattr(current_user, "is_authenticated", False):
                abort(401)

        component = find_component(component_id)
        if component is None or not isinstance(component, _Camera):
            abort(404)
        if not getattr(component, "preview", False):
            # preview = false → camera is not published to HTTP clients.
            abort(404)

        # Prefer the still FRAME channel; fall back to whichever bytes channel
        # the camera exposes (a stream-only camera still has a single frame).
        channel = None
        for key in (_Camera.FRAME, _Camera.STREAM, _Camera.MOTION):
            if key in component.data:
                channel = component.data.get(key)
                break
        if channel is None:
            abort(404)

        frame = channel.value
        if not isinstance(frame, (bytes, bytearray)):
            # No frame cached yet — the read loop hasn't populated the channel.
            abort(503)

        response = Response(bytes(frame), mimetype="image/jpeg")
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response
