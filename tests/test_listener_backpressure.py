# -*- coding: utf-8 -*-
"""
tests.test_listener_backpressure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Back-pressure for slow listeners: a listener still running is skipped by
``ListenerContext.notify`` (its pending update coalesces onto the next tick
instead of piling duplicate runs onto the executor), the overlap warning
fires once per episode, and an optional per-listener ``interval`` cooldown
suppresses ``has_update`` until it has elapsed since the last completion.
"""

from __future__ import annotations

import logging
import threading

import pandas as pd
from lories._core._context import _Context
from lories.data.listeners import Listener, ListenerContext

T0 = pd.Timestamp("2024-01-01T00:00:00Z")


class _FakeChannel:
    """The slice of a channel that listener dispatch reads: id, validity, timestamp."""

    def __init__(self, id: str, timestamp: pd.Timestamp = T0, valid: bool = True) -> None:
        self.id = id
        self._timestamp = timestamp
        self._valid = valid

    @property
    def timestamp(self) -> pd.Timestamp:
        return self._timestamp

    def is_valid(self) -> bool:
        return self._valid


class _FakeChannels(list):
    @property
    def ids(self):
        return [c.id for c in self]

    def to_frame(self, unique: bool = False, states: bool = False) -> pd.DataFrame:
        return pd.DataFrame()


class _Listeners(ListenerContext):
    """A ``ListenerContext`` without the ``TaskContext`` plumbing ``notify`` never touches."""

    def __init__(self) -> None:
        _Context.__init__(self)  # set up the entity map without asserting a task context
        self._logger = logging.getLogger("lories.data.listeners.context")


def _listener(id: str, channels: _FakeChannels, how: str = "any", interval=None, function=None) -> Listener:
    return Listener(id, id.split(".")[-1], function or (lambda df: None), channels, how=how, interval=interval)


def test_notify_skips_locked_listener_and_warns_once(caplog):
    channel = _FakeChannel("sys.x")
    channels = _FakeChannels([channel])

    started = threading.Event()
    release = threading.Event()

    def slow(df):
        started.set()
        assert release.wait(timeout=5)

    listener = _listener("sys.slow", channels, function=slow)
    listeners = _Listeners()
    listeners._add(listener)

    worker = threading.Thread(target=listener, args=(T0,))
    worker.start()
    try:
        assert started.wait(timeout=5)
        assert listener.locked()

        with caplog.at_level(logging.WARNING, logger="lories.data.listeners.context"):
            # Two ticks land while the run is still in flight: skipped both times,
            # so no duplicate run is dispatched, and the warning fires once.
            assert listener not in listeners.notify(channel)
            assert listener not in listeners.notify(channel)
        warnings = [r for r in caplog.records if "not finished" in r.getMessage()]
        assert len(warnings) == 1
    finally:
        release.set()
        worker.join(timeout=5)
    assert not listener.locked()


def test_notify_returns_released_listener_with_pending_update():
    channel = _FakeChannel("sys.x")
    channels = _FakeChannels([channel])

    listener = _listener("sys.l", channels)
    listeners = _Listeners()
    listeners._add(listener)

    # Run once so the completion timestamp advances to T0 and the lock releases.
    listener(T0)
    assert not listener.locked()

    # Nothing pending yet: the channel has not moved past the last completion.
    assert listener not in listeners.notify(channel)

    # A newer update arriving after release is the coalesced follow-up run.
    channel._timestamp = T0 + pd.Timedelta(seconds=10)
    assert listener in listeners.notify(channel)


def test_cooldown_suppresses_update_until_interval_elapsed():
    channel = _FakeChannel("sys.x")
    channels = _FakeChannels([channel])
    listener = _listener("sys.l", channels, interval=pd.Timedelta(minutes=5))

    listener(T0)  # last completion at T0

    channel._timestamp = T0 + pd.Timedelta(minutes=2)  # inside the cooldown window
    assert not listener.has_update()

    channel._timestamp = T0 + pd.Timedelta(minutes=6)  # past T0 + interval
    assert listener.has_update()


def test_interval_none_keeps_immediate_cadence():
    channel = _FakeChannel("sys.x")
    listener = _listener("sys.l", _FakeChannels([channel]))

    listener(T0)
    channel._timestamp = T0 + pd.Timedelta(seconds=1)
    assert listener.has_update()


def test_interval_string_is_parsed_to_timedelta():
    listener = _listener("sys.l", _FakeChannels([_FakeChannel("sys.x")]), interval="5min")
    assert listener._interval == pd.Timedelta(minutes=5)


def test_overlap_warning_resets_after_completion():
    listener = _listener("sys.l", _FakeChannels([_FakeChannel("sys.x")]))

    # One episode: the first skip warns, the next stays quiet.
    assert listener._should_warn_overlap() is True
    assert listener._should_warn_overlap() is False

    # A completed run ends the episode, so the next overlap warns again.
    listener(T0)
    assert listener._should_warn_overlap() is True
