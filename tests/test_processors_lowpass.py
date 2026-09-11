# -*- coding: utf-8 -*-
"""
tests.test_processors_lowpass
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``LowPass`` is a first-order exponential filter with time constant ``tau``,
independent of the sample interval.
"""

from __future__ import annotations

import math

import pytest

import pandas as pd
from lories.core.configs import ConfigurationError
from lories.data.processors import LowPass, ProcessingError, Processor

T0 = pd.Timestamp("2026-07-10 12:00:00", tz="UTC")


def test_first_sample_passes_through():
    proc = LowPass(key="lp", tau="30s")
    assert proc(T0, 5.0) == pytest.approx(5.0)


def test_regular_step_matches_exponential_formula():
    proc = LowPass(key="lp", tau="30s")
    assert proc(T0, 0.0) == pytest.approx(0.0)
    # dt == tau: alpha = 1 - exp(-1)
    expected = 100.0 * (1 - math.exp(-1))
    assert proc(T0 + pd.Timedelta(seconds=30), 100.0) == pytest.approx(expected)


def test_gap_much_longer_than_tau_snaps_to_new_value():
    proc = LowPass(key="lp", tau="30s")
    assert proc(T0, 0.0) == pytest.approx(0.0)
    assert proc(T0 + pd.Timedelta(hours=2), 500.0) == pytest.approx(500.0)


def test_zero_time_step_is_skipped_and_state_unchanged():
    proc = LowPass(key="lp", tau="30s")
    assert proc(T0, 10.0) == pytest.approx(10.0)
    assert proc(T0, 20.0) is Processor.SKIP
    # state is still 10.0 at T0, not 20.0: the next step must resume from there
    expected = 10.0 + (1 - math.exp(-1)) * (110.0 - 10.0)
    assert proc(T0 + pd.Timedelta(seconds=30), 110.0) == pytest.approx(expected)


def test_non_numeric_value_raises():
    proc = LowPass(key="lp", tau="30s")
    with pytest.raises(ProcessingError):
        proc(T0, "n/a")


def test_missing_tau_raises_configuration_error():
    with pytest.raises(ConfigurationError):
        LowPass(key="lp")


@pytest.mark.parametrize("tau", ["1M", "0s", "abc"])
def test_invalid_tau_raises_configuration_error(tau):
    with pytest.raises(ConfigurationError):
        LowPass(key="lp", tau=tau)
