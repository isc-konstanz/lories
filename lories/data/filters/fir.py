# -*- coding: utf-8 -*-
"""
lories.data.filters.fir
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any

from lories.core.typing import Timestamp
from lories.data.filters import Filter


class Fir(Filter):
    coefficients: list[float]
    _buffer: list[float]

    def __init__(self, _type, **configs):
        super().__init__(_type, **configs)

        self.coefficients = self.get("coefficients", [1])
        self._buffer = [0.0] * len(self.coefficients)

    def filter(self, timestamp: Timestamp, value: Any) -> Any:
        if not isinstance(value, ( int, float )):
            raise ValueError("IIR filter only supports numeric values")

        self._buffer.pop()
        self._buffer.insert(0, value)

        return sum(c * v for c, v in zip(self.coefficients, self._buffer))
