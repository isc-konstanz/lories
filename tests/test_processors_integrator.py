# -*- coding: utf-8 -*-
"""
tests.test_processors_integrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Integrator`` accumulates ``last * dt / per * factor`` (rectangle rule on the
previous sample, per hour by default) and SKIPs whenever no interval can be
formed.
"""

from __future__ import annotations

import pytest

import pandas as pd
from lories.data.processors import Integrator, ProcessingError, Processor

T0 = pd.Timestamp("2026-07-10 12:00:00", tz="UTC")
T1 = pd.Timestamp("2026-07-10 12:00:30", tz="UTC")
T2 = pd.Timestamp("2026-07-10 12:01:00", tz="UTC")
T3 = pd.Timestamp("2026-07-10 12:03:00", tz="UTC")


def test_first_sample_only_seeds():
    integ = Integrator(key="integ")
    assert integ(T0, 1000.0) is Processor.SKIP


def test_integral_is_per_hour_on_the_previous_value():
    integ = Integrator(key="integ")
    integ(T0, 1200.0)
    # 1200 W held for 30 s = 10 Wh; the current value is not used yet
    assert integ(T1, 0.0) == pytest.approx(10.0)
    assert integ(T2, 3600.0) == pytest.approx(10.0)
    assert integ(T3, 0.0) == pytest.approx(10.0 + 3600.0 * 2 / 60)


def test_constant_input_integrates_linearly_regardless_of_sampling():
    fast = Integrator(key="fast")
    slow = Integrator(key="slow")
    fast(T0, 60.0)
    slow(T0, 60.0)
    for i in range(1, 7):
        result = fast(T0 + pd.Timedelta(seconds=30 * i), 60.0)
    assert result == pytest.approx(3.0)
    assert slow(T3, 60.0) == pytest.approx(3.0)


def test_factor_and_per_scale_the_integral():
    integ = Integrator(key="integ", factor=0.001, per="1min")
    integ(T0, 60.0)
    assert integ(T2, 60.0) == pytest.approx(0.06)  # 60 per-minute units held for 1 min, in kilo


def test_gap_beyond_max_gap_resets_to_cold_start():
    integ = Integrator(key="integ", max_gap="1min")
    integ(T0, 1200.0)
    assert integ(T1, 1200.0) == pytest.approx(10.0)
    assert integ(T3, 1200.0) is Processor.SKIP
    assert integ(T3 + pd.Timedelta(seconds=30), 1200.0) == pytest.approx(10.0)


def test_duplicate_and_backwards_timestamps_are_skipped():
    integ = Integrator(key="integ")
    integ(T1, 1200.0)
    assert integ(T1, 1200.0) is Processor.SKIP
    assert integ(T0, 1200.0) is Processor.SKIP
    assert integ(T1, 0.0) == pytest.approx(10.0)


def test_non_numeric_value_raises():
    integ = Integrator(key="integ")
    with pytest.raises(ProcessingError):
        integ(T0, "n/a")
