# -*- coding: utf-8 -*-
"""
lories.data.filters.basic_filters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories.data.filters import IirFilter, FirFilter


class LowPassFilter(IirFilter):
    def __init__(self, _type, id = None, key = None, name = None, enabled = True, **configs):
        decay = configs.get("decay", 0.5)
        configs["numerator"] = [1 - decay]
        configs["denominator"] = [1, -decay]

        super().__init__(_type, id, key, name, enabled, **configs)


class MovingAverageFilter(FirFilter):
    def __init__(self, _type, id = None, key = None, name = None, enabled = True, **configs):
        length = configs.get("length", 1)
        configs["coefficients"] = [1 / length] * length

        super().__init__(_type, id, key, name, enabled, **configs)



