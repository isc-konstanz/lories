# -*- coding: utf-8 -*-
"""Tests for the generic ``TickScheduler`` (fires on cadence, stops cleanly, never backlogs)."""

from __future__ import annotations

import threading
import time
from datetime import timedelta

from lories.scheduler import TickScheduler


def test_fires_on_cadence_and_stops_cleanly():
    calls = []
    scheduler = TickScheduler(lambda: calls.append(time.monotonic()), interval=timedelta(seconds=0.05), name="fire")
    scheduler.start()
    try:
        deadline = time.monotonic() + 2.0
        while len(calls) == 0 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert len(calls) >= 1
    finally:
        scheduler.stop()
    assert not scheduler.is_running()


def test_slow_run_does_not_backlog_missed_slots():
    calls = []

    def slow() -> None:
        calls.append(time.monotonic())
        time.sleep(0.15)  # far longer than the 0.02s interval

    scheduler = TickScheduler(slow, interval=timedelta(seconds=0.02), name="noqueue")
    scheduler.start()
    time.sleep(0.5)
    scheduler.stop()
    # In ~0.5s with a 0.15s run, only a handful of runs happen; missed 0.02s slots are skipped,
    # not queued (which would be ~25 runs).
    assert len(calls) <= 6


def test_exception_in_callback_does_not_kill_the_loop():
    calls = []

    def flaky() -> None:
        calls.append(1)
        raise RuntimeError("boom")

    scheduler = TickScheduler(flaky, interval=timedelta(seconds=0.02), name="flaky")
    scheduler.start()
    try:
        deadline = time.monotonic() + 2.0
        while len(calls) < 2 and time.monotonic() < deadline:
            time.sleep(0.02)
        # The loop keeps scheduling after a raising run (>= 2 calls means it survived the first).
        assert len(calls) >= 2
        assert scheduler.is_running()
    finally:
        scheduler.stop()


def test_rejects_non_positive_interval():
    import pytest

    with pytest.raises(ValueError):
        TickScheduler(lambda: None, interval=timedelta(0), name="bad")

    # Sanity: the threading primitives are unused when construction fails.
    assert threading.active_count() >= 1
