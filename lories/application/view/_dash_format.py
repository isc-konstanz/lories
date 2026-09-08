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

import math
from typing import Any

# Channel units that the UI should render as image previews. Compared
# case-insensitively after stripping whitespace.
IMAGE_UNITS: frozenset[str] = frozenset(
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


# Magnitudes rendered in plain decimal notation. Outside this range the
# value falls back to scientific notation (``1.23e-05``, ``1.23e+07``).
PLAIN_MIN: float = 1e-3
PLAIN_MAX: float = 1e7


def format_number(value: float, digits: int = 3) -> str:
    """Render a scalar for the dash UI with ``digits`` significant figures.

    Values with ``PLAIN_MIN <= |value| < PLAIN_MAX`` print in plain decimal
    notation with at least two decimals, so power and energy readings in
    the kilo/mega range stay readable and their decimal points line up in
    a right-aligned column (``1234.00``, ``12.35``, ``0.50``). Values below
    one keep ``digits`` significant figures instead, so ``0.0123`` does not
    collapse to ``0.01``. Zero prints as ``0.00``. Anything smaller or
    larger than the plain range prints in scientific notation with
    ``digits`` significant figures (``5.00e-04``, ``1.23e+07``).
    """
    v = float(value)
    if math.isnan(v):
        return "—"
    if math.isinf(v):
        return str(v)
    if v == 0:
        return f"{0.0:.{digits - 1}f}"
    magnitude = abs(v)
    if magnitude < PLAIN_MIN or magnitude >= PLAIN_MAX:
        return f"{v:.{digits - 1}e}"
    decimals = max(2, digits - 1 - math.floor(math.log10(magnitude)))
    return f"{v:.{decimals}f}"


# Header columns of a channel accordion item: value right-aligned, unit
# left-aligned, state left-aligned, each with a fixed minimum width so the
# columns line up across rows and the value/unit boundary is unambiguous.
HEADER_VALUE_STYLE: dict[str, str] = {
    "display": "inline-block",
    "minWidth": "7rem",
    "textAlign": "right",
    "fontVariantNumeric": "tabular-nums",
}
HEADER_UNIT_STYLE: dict[str, str] = {
    "display": "inline-block",
    "minWidth": "3.5rem",
    "textAlign": "left",
    "marginRight": "1rem",
}
HEADER_STATE_STYLE: dict[str, str] = {
    "display": "inline-block",
    "minWidth": "6rem",
    "textAlign": "left",
    "marginRight": "1rem",
}


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
    if unit in IMAGE_UNITS:
        return "(image)"

    try:
        size = len(value) if isinstance(value, (bytes, bytearray, memoryview)) else 0
    except Exception:  # noqa: BLE001
        size = 0
    if size <= 0:
        return "(binary)"
    return f"({size:,} bytes)"
