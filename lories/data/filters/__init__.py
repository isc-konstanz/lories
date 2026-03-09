# -*- coding: utf-8 -*-
"""
lories.data.filters
~~~~~~~~~~~~~~~~~~~


"""

from .filter import Filter  # noqa: F401

from . import iir
from .iir import Iir

from . import fir
from .fir import Fir

import importlib

FILTERS = [
    "lowpass",
    "movingaverage"
]

for import_filter in FILTERS:
    try:
        importlib.import_module(f".{import_filter}", "lories.data.filters")

    except ModuleNotFoundError:
        # TODO: Implement meaningful logging here
        pass

del importlib