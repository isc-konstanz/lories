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


@register_processor_type("integrator", "integrate", "counter")
class Integrator(Processor):
    TYPE: str = "integrator"

    _integral: float
    _factor: float

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._integral = 0

    def process(self, timestamp: Timestamp, value: Any, factor: float = 1, **kwargs) -> Optional[float]:
        if not is_float(value):
            raise ProcessingError("Currently unable to integrate values other than float or int")
        if self._integral <= value:
            self._integral = value
        self._integral += value * factor

        return self._integral
