# -*- coding: utf-8 -*-
"""
lories.application.view._dash_format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Small private helpers shared by the Dash UI pages for rendering
channel values. Kept under ``application/view/`` rather than
``lories.util`` so non-UI consumers don't pull in stringified
display logic.
"""

from __future__ import annotations

from typing import Any

# Channel units that the UI should render as image previews. Compared
# case-insensitively after stripping whitespace.
_IMAGE_UNITS: frozenset[str] = frozenset(
    {
        "png",
        "jpg",
        "jpeg",
        "webp",
        "gif",
        "svg",
        "bmp",
        "tiff",
        "tif",
    }
)


def format_bytes_label(channel: Any, value: Any) -> str:
    """Human-readable summary for a ``bytes`` channel in the dash UI.

    Three modes, picked off the channel's own metadata so each
    consumer (`SoilSimulation.SOIL_PROGRESS_IMAGE` vs
    `SoilSimulation.SIMULATION_STATE` vs a camera feed) self-tags:

    - ``"(streaming)"`` — the channel declared ``stream = true`` in its
      config block (live MJPEG / WebRTC).
    - ``"(image)"`` — the channel's unit is a known image-format token
      (``"png"``, ``"jpeg"``, …). The Dash UI can decode and display
      these inline.
    - ``"(<size> bytes)"`` — anything else: opaque binary blob
      (serialised state, archived numpy ``.npz``, etc.). The byte
      count makes "is this populated and roughly the expected
      size?" a one-glance check.
    """
    try:
        is_stream = bool(channel.get("stream", default=False))
    except Exception:  # noqa: BLE001
        is_stream = False
    if is_stream:
        return "(streaming)"

    unit = (getattr(channel, "unit", "") or "").strip().lower()
    if unit in _IMAGE_UNITS:
        return "(image)"

    try:
        size = len(value) if isinstance(value, (bytes, bytearray, memoryview)) else 0
    except Exception:  # noqa: BLE001
        size = 0
    if size <= 0:
        return "(binary)"
    return f"({size:,} bytes)"
