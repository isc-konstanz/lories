# -*- coding: utf-8 -*-
"""
tests.test_camera_stream_gate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A suspended camera stream keeps draining frames but publishes none, so no
processor or listener runs; the connector forwards the flag to its stream and
tolerates having none.
"""

from __future__ import annotations

from threading import Event

from lories.connectors.cameras.camera import CameraConnector
from lories.connectors.cameras.stream import CameraStream


class _FakeChannel:
    def __init__(self) -> None:
        self.values = []

    @property
    def value(self):
        return self.values[-1] if self.values else None

    @value.setter
    def value(self, value) -> None:
        self.values.append(value)


def _stream() -> CameraStream:
    # __init__ spawns a Manager process and allocates shared memory the gate never touches.
    stream = object.__new__(CameraStream)
    stream._suspended = Event()
    return stream


def test_suspended_stream_drains_without_publishing():
    stream = _stream()
    channels = [_FakeChannel(), _FakeChannel()]
    assert stream._publish(channels, b"one")
    stream.suspend()
    assert stream.is_suspended()
    assert not stream._publish(channels, b"two")
    stream.resume()
    assert not stream.is_suspended()
    assert stream._publish(channels, b"three")
    assert all(channel.values == [b"one", b"three"] for channel in channels)


class _FakeStream:
    def __init__(self) -> None:
        self.calls = []

    def suspend(self) -> None:
        self.calls.append("suspend")

    def resume(self) -> None:
        self.calls.append("resume")


class _Connector(CameraConnector):
    def read_frame(self, resource) -> bytes:
        return b""


def test_connector_forwards_to_its_stream_and_tolerates_none():
    connector = object.__new__(_Connector)
    connector.suspend()
    assert connector.is_suspended()

    stream = _FakeStream()
    connector._stream = stream
    connector.resume()
    assert not connector.is_suspended()
    connector.suspend()
    assert stream.calls == ["resume", "suspend"]
