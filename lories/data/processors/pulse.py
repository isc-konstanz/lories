# -*- coding: utf-8 -*-
"""
lories.data.processors.pulse
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Optional

from lories.core.typing import Timestamp
from lories.data.processors import Processor, register_processor_type


@register_processor_type("pulse", "pulse_count", "edge_count")
class PulseCount(Processor):
    """
    Per-event pulse processor.

    Emits ``factor`` whenever the input value is truthy (e.g. a rising-edge
    listener event on a digital input). Falsy values pass through as ``0``.
    Combined with logger ``aggregate="sum"`` this turns a debounced edge
    listener into a per-bucket volume / count without relying on a hardware
    pulse counter — debouncing belongs upstream (e.g. the RevPi listener's
    ``cooldown`` parameter).
    """

    TYPE: str = "pulse"

    def process(self, timestamp: Timestamp, value: Any, factor: float = 1.0, **kwargs) -> Optional[float]:
        return float(factor) if bool(value) else 0.0
