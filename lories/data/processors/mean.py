# -*- coding: utf-8 -*-
"""
lories.data.processors.mean
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import deque
from typing import Any, Deque, Optional, Tuple

import pandas as pd
from lories.core.configs import ConfigurationError
from lories.core.typing import Timestamp
from lories.data.processors import ProcessingError, Processor, register_processor_type
from lories.data.processors._duration import elapsed_seconds, to_seconds
from lories.util import is_float


@register_processor_type("mean", "moving_average")
class Mean(Processor):
    """
    Time-weighted moving average over the trailing ``window``.

    Each reading is weighted backward: it stands for the interval that just
    elapsed since the previous reading, clipped to ``window``. This is the
    opposite of the integrator's forward hold, because a smoothed value is
    meant to describe now, not what led up to it. Samples whose interval ends
    at or before the window start are dropped, and the oldest remaining
    sample is clipped to fill the window down to its start. The returned
    value is the time-weighted sum of the remaining samples divided by
    ``window``: the weights always sum to exactly ``window``, so the
    first-ever sample passes through unchanged and a gap longer than
    ``window`` snaps to the new reading.
    """

    TYPE: str = "mean"

    _samples: Deque[Tuple[Timestamp, float]]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        window = kwargs.get("window")
        if window is None:
            raise ConfigurationError("Mean processor requires a 'window' configuration")
        to_seconds(window)
        self._samples = deque()

    def process(self, timestamp: Timestamp, value: Any, window: Any = None, **kwargs) -> Optional[float]:
        if window is None:
            raise ConfigurationError("Mean processor requires a 'window' configuration")
        if not is_float(value):
            raise ProcessingError("Currently unable to average values other than float or int")
        value = float(value)

        samples = self._samples
        if samples and elapsed_seconds(samples[-1][0], timestamp) <= 0:
            return Processor.SKIP
        samples.append((timestamp, value))

        window_seconds = to_seconds(window)
        start = timestamp - pd.Timedelta(seconds=window_seconds)
        while samples and elapsed_seconds(start, samples[0][0]) <= 0:
            samples.popleft()

        total = 0.0
        previous = start
        for sample_timestamp, sample_value in samples:
            weight = elapsed_seconds(previous, sample_timestamp)
            total += sample_value * weight
            previous = sample_timestamp
        return total / window_seconds
