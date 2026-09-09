# -*- coding: utf-8 -*-
"""
lories.data.processors.differentiator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Optional

from lories.core.typing import Timestamp
from lories.data.processors import ProcessingError, Processor, register_processor_type
from lories.data.processors._duration import elapsed_seconds, to_seconds
from lories.util import is_float


@register_processor_type("differentiator", "difference", "diff")
class Differentiator(Processor):
    """
    Time derivative of a sampled value.

    Emits ``(value - last) / (dt / per) * factor``, the mean rate of change since
    the previous sample, expressed per ``per`` (default ``"1h"``): a kWh counter
    becomes kW, ``factor = 1000`` makes it W. A missed read yields the mean rate
    over the gap. ``SKIP`` on the first sample, on a counter reset when
    ``increasing`` is set, on a non-positive time step and on a gap longer than
    ``max_gap``; each of these reseeds the reference sample.
    """

    TYPE: str = "differentiator"

    _last: Optional[float]
    _last_timestamp: Optional[Timestamp]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._last = None
        self._last_timestamp = None

    def process(
        self,
        timestamp: Timestamp,
        value: Any,
        factor: float = 1,
        per: Any = "1h",
        max_gap: Optional[Any] = None,
        increasing: bool = False,
        **kwargs,
    ) -> Optional[float]:
        if not is_float(value):
            raise ProcessingError("Currently unable to differentiate values other than float or int")
        value = float(value)
        last, last_timestamp = self._last, self._last_timestamp
        self._last, self._last_timestamp = value, timestamp

        if last is None or (increasing and last > value):
            return Processor.SKIP
        dt = elapsed_seconds(last_timestamp, timestamp)
        if dt <= 0:
            return Processor.SKIP
        if max_gap is not None and dt > to_seconds(max_gap):
            return Processor.SKIP
        return (value - last) / (dt / to_seconds(per)) * factor
