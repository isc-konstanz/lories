# -*- coding: utf-8 -*-
"""
tests.test_camera_stream_gate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A muted channel stops receiving stream frames, so its processors and listeners
idle while the other channels of the same stream stay live; the connector
forwards the mute to its stream and tolerates having none.
"""

from __future__ import annotations

from lories.connectors.cameras.camera import CameraConnector
from lories.connectors.cameras.stream import CameraStream


class _FakeChannel:
    def __init__(self, id: str) -> None:
        self.id = id
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
    stream._muted = frozenset()
    return stream


def test_muted_channel_idles_while_siblings_stay_live():
    stream = _stream()
    live = _FakeChannel("camera.stream")
    motion = _FakeChannel("camera.motion")
    assert stream._publish([live, motion], b"one") == 2

    stream.mute(motion)
    assert stream.is_muted(motion) and not stream.is_muted(live)
    assert stream._publish([live, motion], b"two") == 1

    stream.unmute("camera.motion")
    assert stream._publish([live, motion], b"three") == 2
    assert live.values == [b"one", b"two", b"three"]
    assert motion.values == [b"one", b"three"]


class _FakeStream:
    def __init__(self) -> None:
        self.calls = []

    def mute(self, *channels) -> None:
        self.calls.append(("mute", set(channels)))

    def unmute(self, *channels) -> None:
        self.calls.append(("unmute", set(channels)))


class _Connector(CameraConnector):
    def read_frame(self, resource) -> bytes:
        return b""


def test_connector_forwards_to_its_stream_and_tolerates_none():
    connector = object.__new__(_Connector)
    connector.mute("camera.motion")
    assert connector.is_muted("camera.motion")

    stream = _FakeStream()
    connector._stream = stream
    connector.unmute("camera.motion")
    assert not connector.is_muted("camera.motion")
    connector.mute("camera.motion")
    assert stream.calls == [("unmute", {"camera.motion"}), ("mute", {"camera.motion"})]
