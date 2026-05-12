# -*- coding: utf-8 -*-
"""
lories.application.view.stream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MJPEG streaming for ``bytes`` channels (e.g. camera streams).

A Flask route at ``/api/stream/<channel_id>`` returns a
``multipart/x-mixed-replace; boundary=frame`` response that the browser
embeds via ``<img src="/api/stream/...">``. No JSON, no base64 — the
browser decodes JPEG frames natively, so frame rate is limited by the
camera and the network rather than by Dash.
"""

from __future__ import annotations

import time
from typing import Callable, Optional

from flask import Flask, Response, abort


def _mjpeg_frames(channel, *, fps_max: int = 60, poll_hz: int = 120, keepalive_s: float = 0.5):
    """Yield multipart MJPEG frames for *channel* until the client disconnects.

    *fps_max* is the upper bound on frames emitted per second; the actual rate
    is whichever is lower of the camera's production rate and this cap. *poll_hz*
    controls how often we check for a new frame — keep it well above *fps_max*
    so latency between camera write and HTTP yield stays low.

    We hold a strong reference to the last frame (``last_value``) so its memory
    cannot be recycled — otherwise CPython could allocate the next frame at the
    same address and the ``is`` check would miss the update (common with
    event-driven channels where frames are seconds apart).

    *keepalive_s* is the maximum gap between yields. On Werkzeug's dev server the
    socket buffer holds the trailing ``\\r\\n--frame\\r\\n`` of an emit until the
    next write pushes it through; without a keepalive, the browser would only
    render frame N when frame N+1 finally arrives — manifesting as a one-event
    display lag on event-driven channels. Re-yielding the same frame at this
    interval flushes that tail through the buffer.
    """
    min_period = 1.0 / max(1, fps_max)
    poll_period = 1.0 / max(1, poll_hz)
    last_value = None
    last_emit = 0.0
    yield b"--frame\r\n"
    try:
        while True:
            value = channel.value
            now = time.monotonic()
            if not isinstance(value, (bytes, bytearray)):
                time.sleep(poll_period)
                continue
            has_new = value is not last_value and (now - last_emit) >= min_period
            needs_keepalive = last_value is not None and (now - last_emit) >= keepalive_s
            if has_new or needs_keepalive:
                if has_new:
                    last_value = value
                last_emit = now
                frame = bytes(last_value)
                yield (
                    b"Content-Type: image/jpeg\r\n"
                    b"Content-Length: " + str(len(frame)).encode("ascii") + b"\r\n\r\n" + frame + b"\r\n--frame\r\n"
                )
            time.sleep(poll_period)
    except GeneratorExit:
        return


def register_stream_routes(
    server: Flask,
    find_channel: Callable[[str], Optional[object]],
    *,
    require_login: bool = False,
) -> None:
    """Register the MJPEG streaming route on *server*.

    Parameters
    ----------
    server:
        The Flask app underlying the Dash server (``Dash.server``).
    find_channel:
        Callable resolving a fully-qualified channel id to a ``Channel``
        instance (or ``None`` if unknown).
    require_login:
        When ``True``, reject requests from unauthenticated users with 401.
    """

    @server.route("/api/stream/<path:channel_id>")
    def stream_route(channel_id: str):  # noqa: WPS430 — Flask requires a named view
        if require_login:
            from flask_login import current_user

            if not getattr(current_user, "is_authenticated", False):
                abort(401)
        channel = find_channel(channel_id)
        if channel is None:
            abort(404)
        if getattr(channel, "type", None) is not bytes:
            abort(400)
        response = Response(
            _mjpeg_frames(channel),
            mimetype="multipart/x-mixed-replace; boundary=frame",
            direct_passthrough=True,
        )
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.headers["X-Accel-Buffering"] = "no"
        return response
