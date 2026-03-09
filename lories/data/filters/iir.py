# -*- coding: utf-8 -*-
"""
lories.data.filters.iir
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any

from lories.core.typing import Timestamp
from lories.data.filters import Filter


class Iir(Filter):
    coefficients: list[float]
    _buffer: list[float]

    def __init__(self, _type, numerator, denominator, **configs):
        super().__init__(_type, **configs)
        self.numerator = numerator
        self.denominator = denominator

        self._buffer_x = [0.0] * len(numerator)
        self._buffer_y = [0.0] * (len(denominator) - 1)

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
