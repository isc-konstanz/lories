# -*- coding: utf-8 -*-
"""
tests.test_connectors_camera_stream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `_stream` subprocess worker must survive transient RTSP read failures by
reopening the capture in place, and only die once the retry budget is spent.

"""

import logging
from multiprocessing.shared_memory import SharedMemory
from threading import Event
from types import SimpleNamespace

import pytest

from lories.connectors.cameras.stream import _stream
from lories.connectors.errors import ConnectionError, ConnectorError


class ScriptedCamera:
    """Duck-typed camera whose read_frame outcomes follow a script; exceptions
    in the script are raised, bytes are returned. Sets the interrupt event once
    the script is exhausted so `_stream` terminates."""

    def __init__(self, read_script, interrupt, connect_script=None):
        self._read_script = list(read_script)
        self._connect_script = list(connect_script or [])
        self._interrupt = interrupt
        self._logger = logging.getLogger("test.camera")
        self.connects = 0
        self.disconnects = 0

    def connect(self, channels):
        self.connects += 1
        if self._connect_script:
            outcome = self._connect_script.pop(0)
            if isinstance(outcome, Exception):
                raise outcome

    def disconnect(self):
        self.disconnects += 1

    def is_connected(self):
        return True

    def read_frame(self, source):
        if not self._read_script:
            self._interrupt.set()
            return b""
        outcome = self._read_script.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        if not self._read_script:
            self._interrupt.set()
        return outcome


class TrippingEvent(Event):
    """Event whose first timed wait sets itself, simulating an interrupt
    arriving during the retry backoff."""

    def wait(self, timeout=None):
        if timeout is not None:
            self.set()
            return True
        return super().wait()


@pytest.fixture
def memory():
    memory = SharedMemory(create=True, size=1024)
    yield memory
    memory.buf.release()
    memory.close()
    try:
        memory.unlink()
    except FileNotFoundError:
        pass


def run_stream(camera, interrupt, memory, **kwargs):
    trigger = Event()
    produced = SimpleNamespace(value=0)
    channels = [SimpleNamespace()]
    kwargs.setdefault("fps", 1000)
    kwargs.setdefault("retry_backoff", 0.01)
    _stream(camera, channels, trigger, interrupt, memory.name, produced, **kwargs)
    return trigger, produced


def test_stream_recovers_from_transient_read_failure(memory):
    interrupt = Event()
    error = ConnectionError(None, "Failed to grab frame")
    camera = ScriptedCamera([error, b"jpegdata"], interrupt)

    trigger, produced = run_stream(camera, interrupt, memory)

    assert produced.value == 1
    assert trigger.is_set()
    length = int.from_bytes(memory.buf[0:4], "little")
    assert bytes(memory.buf[4 : 4 + length]) == b"jpegdata"
    # one recovery reopen plus the initial connect; one recovery disconnect
    # plus the final teardown disconnect
    assert camera.connects == 2
    assert camera.disconnects == 2


def test_stream_counts_failed_reopen_and_still_recovers(memory):
    interrupt = Event()
    read_error = ConnectionError(None, "Failed to grab frame")
    reopen_error = ConnectionError(None, "Cannot open RTSP stream")
    camera = ScriptedCamera(
        [read_error, read_error, b"jpegdata"],
        interrupt,
        connect_script=[None, reopen_error, None],
    )

    trigger, produced = run_stream(camera, interrupt, memory)

    assert produced.value == 1
    assert camera.connects == 3


def test_stream_raises_after_retry_budget_exhausted(memory):
    interrupt = Event()
    errors = [ConnectionError(None, "Failed to grab frame") for _ in range(4)]
    camera = ScriptedCamera(errors, interrupt)

    with pytest.raises(ConnectorError):
        run_stream(camera, interrupt, memory, retries=3)

    assert camera.connects == 4  # initial + 3 reopen attempts
    assert camera.disconnects == 4  # 3 recovery disconnects + final teardown


def test_stream_returns_when_interrupted_during_backoff(memory):
    interrupt = TrippingEvent()
    error = ConnectionError(None, "Failed to grab frame")
    camera = ScriptedCamera([error, b"never-read"], interrupt)

    trigger, produced = run_stream(camera, interrupt, memory)

    assert produced.value == 0
    assert camera.connects == 1  # no reopen after the interrupt tripped
    assert camera.disconnects == 2  # recovery disconnect + final teardown
