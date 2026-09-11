# -*- coding: utf-8 -*-
"""
tests.test_processors_differentiator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Differentiator`` is a time derivative per ``per`` (default one hour). Frames
that cannot form a rate must be SKIPped, never returned as ``None`` (a
valid-state ``None`` fails the whole connector read).
"""

from __future__ import annotations

import pytest

import pandas as pd
from lories.core.configs import ConfigurationError
from lories.data.processors import Differentiator, ProcessingError, Processor

T0 = pd.Timestamp("2026-07-10 12:00:00", tz="UTC")
T1 = pd.Timestamp("2026-07-10 12:00:30", tz="UTC")
T2 = pd.Timestamp("2026-07-10 12:01:00", tz="UTC")
T3 = pd.Timestamp("2026-07-10 12:03:00", tz="UTC")


def test_first_sample_is_skipped():
    deriv = Differentiator(key="deriv")
    assert deriv(T0, 5.0) is Processor.SKIP


def test_rate_is_per_hour_by_default():
    deriv = Differentiator(key="deriv")
    assert deriv(T0, 5.0) is Processor.SKIP
    # 2.5 units in 30 s = 300 units per hour
    assert deriv(T1, 7.5) == pytest.approx(300.0)


def test_factor_scales_the_rate():
    deriv = Differentiator(key="deriv", factor=1000.0)
    deriv(T0, 5.0)
    assert deriv(T1, 7.5) == pytest.approx(300_000.0)


def test_rate_does_not_depend_on_sample_interval():
    fast = Differentiator(key="fast")
    slow = Differentiator(key="slow")
    fast(T0, 0.0)
    slow(T0, 0.0)
    # Same counter slope (1 unit per minute) sampled at 30 s and at 3 min
    assert fast(T1, 0.5) == pytest.approx(60.0)
    assert slow(T3, 3.0) == pytest.approx(60.0)


@pytest.mark.parametrize("per, expected", [("1min", 5.0), ("15min", 75.0), ("1s", 5.0 / 60), ("1h", 300.0)])
def test_per_sets_the_time_base(per, expected):
    deriv = Differentiator(key="deriv", per=per)
    deriv(T0, 5.0)
    assert deriv(T1, 7.5) == pytest.approx(expected)


def test_gap_yields_mean_rate_over_the_gap():
    deriv = Differentiator(key="deriv")
    deriv(T0, 0.0)
    assert deriv(T3, 6.0) == pytest.approx(120.0)  # 6 units in 3 min


def test_gap_beyond_max_gap_is_skipped_and_reseeds():
    deriv = Differentiator(key="deriv", max_gap="1min")
    deriv(T0, 0.0)
    assert deriv(T3, 6.0) is Processor.SKIP
    assert deriv(T3 + pd.Timedelta(seconds=30), 7.0) == pytest.approx(120.0)


def test_gap_within_max_gap_passes():
    deriv = Differentiator(key="deriv", max_gap="5min")
    deriv(T0, 0.0)
    assert deriv(T3, 6.0) == pytest.approx(120.0)


def test_duplicate_timestamp_is_skipped():
    deriv = Differentiator(key="deriv")
    deriv(T0, 5.0)
    assert deriv(T0, 7.5) is Processor.SKIP
    assert deriv(T1, 8.5) == pytest.approx(120.0)  # delta from the reseeded 7.5


def test_backwards_timestamp_is_skipped():
    deriv = Differentiator(key="deriv")
    deriv(T1, 5.0)
    assert deriv(T0, 7.5) is Processor.SKIP


def test_increasing_counter_reset_is_skipped():
    deriv = Differentiator(key="deriv", increasing=True)
    assert deriv(T0, 100.0) is Processor.SKIP
    assert deriv(T1, 40.0) is Processor.SKIP  # counter reset: no valid delta
    assert deriv(T2, 41.0) == pytest.approx(120.0)  # rates resume from the reset value


def test_decreasing_value_without_increasing_is_negative_rate():
    deriv = Differentiator(key="deriv")
    deriv(T0, 10.0)
    assert deriv(T2, 9.0) == pytest.approx(-60.0)


def test_non_numeric_value_raises():
    deriv = Differentiator(key="deriv")
    with pytest.raises(ProcessingError):
        deriv(T0, "n/a")


@pytest.mark.parametrize("per", ["1M", "2W", "1Y", "0s", "soon"])
def test_invalid_per_raises_configuration_error(per):
    deriv = Differentiator(key="deriv", per=per)
    deriv(T0, 5.0)
    with pytest.raises(ConfigurationError):
        deriv(T1, 7.5)
