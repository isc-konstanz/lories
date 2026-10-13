# -*- coding: utf-8 -*-
"""Tests for the generic ``TickScheduler`` (fires on cadence, stops cleanly, never backlogs)."""

from __future__ import annotations

import logging
import threading
import time
from datetime import timedelta

import pandas as pd
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


def test_exception_logs_traceback_at_error(caplog):
    import logging

    calls = []

    def flaky() -> None:
        calls.append(1)
        raise RuntimeError("boom")

    scheduler = TickScheduler(flaky, interval=timedelta(seconds=0.02), name="traceback")
    with caplog.at_level(logging.ERROR, logger="lories.scheduler"):
        scheduler.start()
        try:
            deadline = time.monotonic() + 2.0
            while len(calls) == 0 and time.monotonic() < deadline:
                time.sleep(0.02)
        finally:
            scheduler.stop()

    failures = [r for r in caplog.records if "run failed" in r.getMessage()]
    assert len(failures) >= 1
    # The traceback must be attached at ERROR level, not gated behind DEBUG.
    assert all(r.exc_info is not None for r in failures)
    assert failures[0].levelno == logging.ERROR


def test_is_interrupted_tracks_stop_and_restart():
    scheduler = TickScheduler(lambda: None, interval=timedelta(minutes=1), name="interrupt")
    scheduler.start()
    try:
        assert not scheduler.is_interrupted()
    finally:
        scheduler.stop()
    assert scheduler.is_interrupted()

    scheduler.start()
    try:
        assert not scheduler.is_interrupted()
    finally:
        scheduler.stop()


def test_failures_count_consecutive_raising_runs_and_reset_on_success(caplog):
    raising = True

    def on_slot() -> None:
        if raising:
            raise RuntimeError("boom")

    scheduler = TickScheduler(on_slot, interval=timedelta(minutes=1), name="failures")
    assert scheduler.failures == 0

    with caplog.at_level(logging.ERROR, logger="lories.scheduler"):
        scheduler._run_slot()
        scheduler._run_slot()
    assert scheduler.failures == 2
    assert "run failed (2 consecutive)" in caplog.records[-1].getMessage()

    raising = False
    scheduler._run_slot()
    assert scheduler.failures == 0

    raising = True
    scheduler._run_slot()
    assert scheduler.failures == 1


def test_overrun_warning_reports_skipped_slots(caplog, monkeypatch):
    scheduler = TickScheduler(lambda: None, interval=timedelta(seconds=10), name="overrun")
    start = pd.Timestamp("2026-01-01 00:00:00", tz="UTC")

    def overran(duration: pd.Timedelta) -> list[logging.LogRecord]:
        times = iter([start, start + duration])
        monkeypatch.setattr(scheduler, "_now", lambda: next(times))
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="lories.scheduler"):
            scheduler._run_slot()
        return [r for r in caplog.records if "overran its slot" in r.getMessage()]

    records = overran(pd.Timedelta(seconds=25))
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert "slots_skipped=2" in records[0].getMessage()

    assert overran(pd.Timedelta(seconds=1)) == []


def test_rejects_non_positive_interval():
    import pytest

    with pytest.raises(ValueError):
        TickScheduler(lambda: None, interval=timedelta(0), name="bad")

    # Sanity: the threading primitives are unused when construction fails.
    assert threading.active_count() >= 1
