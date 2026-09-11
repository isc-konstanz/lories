# -*- coding: utf-8 -*-
"""
lories.data.processors.integrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Optional

from lories.core.typing import Timestamp
from lories.data.processors import ProcessingError, Processor, register_processor_type
from lories.data.processors._duration import elapsed_seconds, to_seconds
from lories.util import is_float


@register_processor_type("integrator", "integrate", "counter")
class Integrator(Processor):
    """
    Time integral of a sampled value, accumulated since the processor started.

    Each step adds ``last * (dt / per) * factor``: the previous sample is held
    until the current one (rectangle rule, the way a meter accumulates a sampled
    power), expressed per ``per`` (default ``"1h"``): W becomes Wh. The first
    sample only seeds the reference and is ``SKIP``ped, as is a non-positive time
    step. A gap longer than ``max_gap`` resets the integral to zero and behaves
    like a cold start. The integral is not persisted across restarts.
    """

    TYPE: str = "integrator"

    _integral: float
    _last: Optional[float]
    _last_timestamp: Optional[Timestamp]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._integral = 0.0
        self._last = None
        self._last_timestamp = None

    def process(
        self,
        timestamp: Timestamp,
        value: Any,
        factor: float = 1,
        per: Any = "1h",
        max_gap: Optional[Any] = None,
        **kwargs,
    ) -> Optional[float]:
        if not is_float(value):
            raise ProcessingError("Currently unable to integrate values other than float or int")
        value = float(value)
        last, last_timestamp = self._last, self._last_timestamp
        self._last, self._last_timestamp = value, timestamp

        if last is None:
            return Processor.SKIP
        dt = elapsed_seconds(last_timestamp, timestamp)
        if dt <= 0:
            return Processor.SKIP
        if max_gap is not None and dt > to_seconds(max_gap):
            self._integral = 0.0
            return Processor.SKIP
        self._integral += last * (dt / to_seconds(per)) * factor
        return self._integral
