# -*- coding: utf-8 -*-
"""
lories.data.filters.ir_filters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any

from lories.core.typing import Timestamp
from lories.data.filters import Filter


class IirFilter(Filter):
    coefficients: list[float]
    _buffer: list[float]

    def __init__(self, _type, id=None, key=None, name=None, enabled=True, **configs):
        self.numerator = configs.get("numerator", [1])
        self.denominator = configs.get("denominator", [1])

        self._buffer_x = [0.0] * len(self.numerator)
        self._buffer_y = [0.0] * (len(self.denominator) - 1)

    def filter(self, timestamp: Timestamp, value: Any) -> Any:
        if not isinstance(value, ( int, float )):
            raise ValueError("IIR filter only supports numeric values")

        # shift input buffer
        self._buffer_x.pop()
        self._buffer_x.insert(0, value)

        numerator_sum = sum(
            b * x for b, x in zip(self.numerator, self._buffer_x)
        )

        denominator_sum = sum(
            a * y for a, y in zip(self.denominator[1:], self._buffer_y)
        )

        y = (numerator_sum - denominator_sum) / self.denominator[0]

        # shift output buffer
        self._buffer_y.pop()
        self._buffer_y.insert(0, y)

        return y
    

class FirFilter(Filter):
    coefficients: list[float]
    _buffer: list[float]

    def __init__(self, _type, id=None, key=None, name=None, enabled=True, **configs):
        super().__init__(_type, id, key, name, enabled, **configs)

        self.coefficients = self.get("coefficients", [1])
        self._buffer = [0.0] * len(self.coefficients)

    def filter(self, timestamp: Timestamp, value: Any) -> Any:
        if not isinstance(value, ( int, float )):
            raise ValueError("IIR filter only supports numeric values")

        self._buffer.pop()
        self._buffer.insert(0, value)

        return sum(c * v for c, v in zip(self.coefficients, self._buffer))
