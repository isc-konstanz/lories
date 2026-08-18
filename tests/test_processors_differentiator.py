# -*- coding: utf-8 -*-
"""Unit tests for ``Differentiator``: frames that cannot form a delta must be SKIPped,
never returned as ``None`` (a valid-state ``None`` fails the whole connector read)."""

from __future__ import annotations

import pytest

import pandas as pd
from lories.data.processors import Differentiator, Processor

T0 = pd.Timestamp("2026-07-10 12:00:00", tz="UTC")
T1 = pd.Timestamp("2026-07-10 12:00:30", tz="UTC")
T2 = pd.Timestamp("2026-07-10 12:01:00", tz="UTC")


def test_first_sample_is_skipped():
    deriv = Differentiator(key="deriv")
    assert deriv(T0, 5.0) is Processor.SKIP


def test_second_sample_yields_scaled_delta():
    deriv = Differentiator(key="deriv", factor=10.0)
    assert deriv(T0, 5.0) is Processor.SKIP
    assert deriv(T1, 7.5) == pytest.approx(25.0)


def test_increasing_counter_reset_is_skipped():
    deriv = Differentiator(key="deriv", increasing=True)
    assert deriv(T0, 100.0) is Processor.SKIP
    assert deriv(T1, 40.0) is Processor.SKIP  # counter reset: no valid delta
    assert deriv(T2, 41.0) == pytest.approx(1.0)  # deltas resume from the reset value
