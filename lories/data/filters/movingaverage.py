# -*- coding: utf-8 -*-
"""
lories.data.filters.movingaverage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories.data.filters import Fir


class MovingAverage(Fir):
    def __init__(self, _type, length, **configs):
        super().__init__(_type, coefficients = [1 / length] * length, **configs)
