# -*- coding: utf-8 -*-
"""
lories.data.processors.lowpass
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import math
from typing import Any, Optional

from lories.core.configs import ConfigurationError
from lories.core.typing import Timestamp
from lories.data.processors import ProcessingError, Processor, register_processor_type
from lories.data.processors._duration import elapsed_seconds, to_seconds
from lories.util import is_float


@register_processor_type("lowpass", "low_pass")
class LowPass(Processor):
    """
    First-order exponential smoothing with time constant ``tau``.

    Each step moves the state toward the new reading by
    ``alpha = 1 - exp(-dt / tau)``, the fraction of the remaining gap crossed
    in the elapsed ``dt`` seconds, so the response does not depend on how
    often the value is sampled. The first sample seeds the state and passes
    through unchanged. A gap much longer than ``tau`` drives ``alpha`` toward
    1 and the state snaps to the new reading.
    """

    TYPE: str = "lowpass"

    _state: Optional[float]
    _last_timestamp: Optional[Timestamp]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        tau = kwargs.get("tau")
        if tau is None:
            raise ConfigurationError("LowPass processor requires a 'tau' configuration")
        to_seconds(tau)
        self._state = None
        self._last_timestamp = None

    def process(self, timestamp: Timestamp, value: Any, tau: Any = None, **kwargs) -> Optional[float]:
        if tau is None:
            raise ConfigurationError("LowPass processor requires a 'tau' configuration")
        if not is_float(value):
            raise ProcessingError("Currently unable to filter values other than float or int")
        value = float(value)

        if self._state is None:
            self._state = value
            self._last_timestamp = timestamp
            return value

        dt = elapsed_seconds(self._last_timestamp, timestamp)
        if dt <= 0:
            return Processor.SKIP
        alpha = 1 - math.exp(-dt / to_seconds(tau))
        self._state += alpha * (value - self._state)
        self._last_timestamp = timestamp
        return self._state
