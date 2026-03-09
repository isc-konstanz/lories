# -*- coding: utf-8 -*-
"""
lories.data.filters.lowpass
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories.data.filters import Iir


class LowPassFilter(Iir):
    def __init__(self, _type, decay=0.5, **configs):
        super().__init__(_type, numerator = [1 - decay], denominator = [1, -decay], **configs)
