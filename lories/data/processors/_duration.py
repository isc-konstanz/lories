# -*- coding: utf-8 -*-
"""
lories.data.processors._duration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Duration arguments shared by the time-aware processors (``per``, ``max_gap``).
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import pandas as pd
from lories.core.configs import ConfigurationError
from lories.core.typing import Timestamp
from lories.util import to_timedelta


@lru_cache(maxsize=64)
def _parse_seconds(value: str) -> float:
    try:
        delta = to_timedelta(value)
    except ValueError as e:
        raise ConfigurationError(f"Invalid duration '{value}': {e}") from e
    if not isinstance(delta, pd.Timedelta):
        # Months, weeks and years have no fixed length; a time base needs one.
        raise ConfigurationError(f"Invalid duration '{value}': only fixed-length units (D, h, min, s, ms) are allowed")
    return delta.total_seconds()


def to_seconds(value: Any) -> float:
    """Accept duration strings like ``"1h"`` / ``"15min"`` or numeric seconds."""
    if isinstance(value, str):
        seconds = _parse_seconds(value)
    else:
        seconds = float(value)
    if seconds <= 0:
        raise ConfigurationError(f"Invalid duration '{value}': must be positive")
    return seconds


def elapsed_seconds(start: Timestamp, end: Timestamp) -> float:
    return (pd.Timestamp(end) - pd.Timestamp(start)).total_seconds()
