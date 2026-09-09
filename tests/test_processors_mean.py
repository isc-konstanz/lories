# -*- coding: utf-8 -*-
"""
tests.test_processors_mean
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Mean`` is a time-weighted moving average over the trailing ``window``.
Readings are weighted backward: each stands for the interval that just
elapsed since the previous reading, clipped to ``window``.
"""

from __future__ import annotations

import pytest

import pandas as pd
from lories.core.configs import ConfigurationError
from lories.data.processors import Mean, ProcessingError, Processor

T0 = pd.Timestamp("2026-07-10 12:00:00", tz="UTC")


def test_single_sample_passes_through():
    proc = Mean(key="mean", window="5min")
    assert proc(T0, 42.0) == pytest.approx(42.0)


def test_regular_sampling_equals_arithmetic_mean():
    proc = Mean(key="mean", window="3min")
    # First sample seeds the state and passes through unchanged.
    assert proc(T0, 10.0) == pytest.approx(10.0)
    # Window is (T0-2min, T0+1min]: 10 covers 2 min, 20 covers 1 min -> (10*120+20*60)/180
    assert proc(T0 + pd.Timedelta(minutes=1), 20.0) == pytest.approx((10.0 * 120 + 20.0 * 60) / 180.0)
    # Window is (T0-1min, T0+2min]: 10, 20 and 30 each cover exactly 1 min -> arithmetic mean
    assert proc(T0 + pd.Timedelta(minutes=2), 30.0) == pytest.approx(20.0)


def test_irregular_sampling_weights_by_time():
    proc = Mean(key="mean", window="10min")
    assert proc(T0, 0.0) == pytest.approx(0.0)
    # 100 stands for the 9 min that just elapsed and dominates; 0 only covers the first minute of the window
    total = 0.0 * 60 + 100.0 * 540
    assert proc(T0 + pd.Timedelta(minutes=9), 100.0) == pytest.approx(total / 600.0)


def test_burst_of_reads_does_not_dominate():
    proc = Mean(key="mean", window="10min")
    proc(T0, 0.0)
    proc(T0 + pd.Timedelta(minutes=9), 100.0)
    proc(T0 + pd.Timedelta(minutes=9, seconds=10), 200.0)
    result = proc(T0 + pd.Timedelta(minutes=9, seconds=20), 300.0)
    # window start is T0-40s: 0 covers 40 s, 100 covers 540 s, 200 and 300 cover 10 s each
    total = 0.0 * 40 + 100.0 * 540 + 200.0 * 10 + 300.0 * 10
    assert result == pytest.approx(total / 600.0)
    # the naive average of the four readings would be 150, the burst average 200: neither applies
    assert result == pytest.approx(98.333333, rel=1e-4)


def test_gap_longer_than_window_snaps_to_new_value():
    proc = Mean(key="mean", window="5min")
    assert proc(T0, 10.0) == pytest.approx(10.0)
    assert proc(T0 + pd.Timedelta(hours=1), 999.0) == pytest.approx(999.0)


def test_oldest_sample_is_clipped_at_window_start():
    proc = Mean(key="mean", window="10min")
    assert proc(T0, 0.0) == pytest.approx(0.0)
    # window start is T0-7min: the first sample is clipped to cover 7 min, not held forever
    total = 0.0 * 420 + 50.0 * 180
    assert proc(T0 + pd.Timedelta(minutes=3), 50.0) == pytest.approx(total / 600.0)


def test_backwards_timestamp_is_skipped_and_next_sample_unaffected():
    proc = Mean(key="mean", window="5min")
    assert proc(T0, 10.0) == pytest.approx(10.0)
    assert proc(T0 - pd.Timedelta(minutes=1), 20.0) is Processor.SKIP
    # the skipped read must not have altered state: window is (T0-4min, T0+1min]
    total = 10.0 * 240 + 30.0 * 60
    assert proc(T0 + pd.Timedelta(minutes=1), 30.0) == pytest.approx(total / 300.0)


def test_non_numeric_value_raises():
    proc = Mean(key="mean", window="5min")
    with pytest.raises(ProcessingError):
        proc(T0, "n/a")


def test_missing_window_raises_configuration_error():
    with pytest.raises(ConfigurationError):
        Mean(key="mean")


@pytest.mark.parametrize("window", ["1M", "0s", "abc"])
def test_invalid_window_raises_configuration_error(window):
    with pytest.raises(ConfigurationError):
        Mean(key="mean", window=window)
