# -*- coding: utf-8 -*-
"""
lories.scheduler
~~~~~~~~~~~~~~~~

A small, self-contained scheduler that runs a callback on an interval/offset-aligned cadence.
It is the domain-agnostic skeleton of the sparcs field-simulation tick: an aligned slot loop, an
interrupt ``Event`` for prompt shutdown, a bounded join, and a non-blocking lock that skips
(never queues) a slot while the previous run is still in flight.
"""

from __future__ import annotations

import logging
import threading
from typing import Callable, Optional

import pandas as pd
import pytz as tz

logger = logging.getLogger(__name__)


class TickScheduler:
    """Run ``on_slot`` on a fixed interval/offset-aligned cadence on a daemon thread.

    Runs are aligned to ``k * interval + offset`` in ``timezone``. A run slower than ``interval``
    does not backlog: the next slot is computed from the moment the run finishes, so missed slots
    are skipped rather than queued. Exceptions raised by ``on_slot`` are logged and swallowed so a
    single failing run never kills the loop.
    """

    _WAIT_MAX_SECONDS: float = 60.0

    def __init__(
        self,
        on_slot: Callable[[], None],
        interval,
        offset=None,
        *,
        name: str = "tick",
        timezone: tz.BaseTzInfo = tz.UTC,
        join_timeout: float = 30.0,
    ) -> None:
        self._on_slot = on_slot
        self._interval = pd.Timedelta(interval)
        if self._interval <= pd.Timedelta(0):
            raise ValueError(f"TickScheduler interval must be positive, got {self._interval}")
        self._offset = pd.Timedelta(offset) if offset is not None else pd.Timedelta(0)
        self._name = name
        self._timezone = timezone
        self._join_timeout = join_timeout

        self._interrupt = threading.Event()
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.is_running():
            return
        self._interrupt.clear()
        self._thread = threading.Thread(target=self._loop, name=f"{self._name}.tick", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._interrupt.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=self._join_timeout)
            if thread.is_alive():
                # Keep the reference so is_running() stays true and a later start() does not spawn
                # a second loop; the daemon thread exits on its own once the run completes.
                logger.warning(
                    "Tick scheduler '%s' did not stop within %ss; leaving it to exit on its own",
                    self._name,
                    self._join_timeout,
                )
                return
        self._thread = None

    def _now(self) -> pd.Timestamp:
        return pd.Timestamp.now(tz=self._timezone)

    def _next_slot(self, now: pd.Timestamp) -> pd.Timestamp:
        interval_seconds = self._interval.total_seconds()
        offset_seconds = self._offset.total_seconds()
        count = (now.timestamp() - offset_seconds) // interval_seconds
        nxt = (count + 1) * interval_seconds + offset_seconds
        return pd.Timestamp(nxt, unit="s", tz="UTC").tz_convert(self._timezone)

    def _loop(self) -> None:
        slot = self._next_slot(self._now())
        while not self._interrupt.is_set():
            now = self._now()
            if now < slot:
                wait = min((slot - now).total_seconds(), self._WAIT_MAX_SECONDS)
                if self._interrupt.wait(timeout=wait):
                    break
                continue
            self._run_slot()
            slot = self._next_slot(self._now())

    def _run_slot(self) -> None:
        if not self._lock.acquire(blocking=False):
            logger.warning("Tick scheduler '%s': previous run still active; skipping slot", self._name)
            return
        try:
            self._on_slot()
        except Exception as e:
            logger.error("Tick scheduler '%s' run failed: %s", self._name, e)
            if logger.getEffectiveLevel() <= logging.DEBUG:
                logger.exception(e)
        finally:
            self._lock.release()
