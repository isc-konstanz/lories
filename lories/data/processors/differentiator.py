# -*- coding: utf-8 -*-
"""
lories.data.processor.integrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Optional

from lories.core.typing import Timestamp
from lories.data.processors import ProcessingError, Processor, register_processor_type
from lories.util import is_float


@register_processor_type("differentiator", "difference", "diff")
class Differentiator(Processor):
    _last: Optional[float]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._last = None

    def process(self, timestamp: Timestamp, value: Any, factor: float = 1, increasing: bool = False) -> Optional[float]:
        if not is_float(value):
            raise ProcessingError("Currently unable to differentiate values other than float or int")
        try:
            if self._last is None or (increasing and self._last > value):
                return value * factor
            return (value - self._last) * factor
        finally:
            self._last = value
