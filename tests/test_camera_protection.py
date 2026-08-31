# -*- coding: utf-8 -*-
"""
tests.test_camera_protection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The protection cycle must not re-trigger itself off the shutter it drives:
motion seen while the shutter is closed is ignored, and ``cooldown`` starts
when the shutter opens, not when it closes, so the opening movement cannot
close it again.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pandas as pd
from lories.components.cameras.camera import Camera
from lories.components.cameras.protection import CameraProtector

T0 = pd.Timestamp("2026-01-01T12:00:00Z")


def _seconds(value: float) -> pd.Timedelta:
    return pd.Timedelta(seconds=value)


class _FakeState:
    """The slice of the state channel the protector touches: value, validity, writes."""

    def __init__(self) -> None:
        self.value = None
        self.writes = []

    def is_valid(self) -> bool:
        return self.value is not None

    def write(self, value) -> None:
        self.writes.append(value)


class _FakeData:
    def __init__(self, state: _FakeState) -> None:
        self._state = state

    def get(self, key):
        return self._state


_MOTION = SimpleNamespace(id="camera.motion")
_APPLES = SimpleNamespace(id="camera.apples")


class _FakeCamera:
    """The slice of the camera connector the protector drives: the per-channel mute."""

    def __init__(self) -> None:
        self.muted = set()

    def mute(self, *channels) -> None:
        self.muted.update(channel.id for channel in channels)

    def unmute(self, *channels) -> None:
        self.muted.difference_update(channel.id for channel in channels)


class _Protector(CameraProtector):
    """A ``CameraProtector`` without the component plumbing the cycle never touches, on a manual clock."""

    def __init__(self, delay: float = 0, cooldown: float = 10) -> None:
        self._id = "camera.protection"
        self._key = "protection"
        self._name = "Protection"
        self._logger = logging.getLogger("lories.components.cameras.protection")
        self.delay = _seconds(delay)
        self.cooldown = _seconds(cooldown)
        self.clock = T0
        self.shutter = _FakeState()
        self.camera = _FakeCamera()
        self._fake_data = _FakeData(self.shutter)

    @property
    def data(self):
        return self._fake_data

    def _consumer_streams(self):
        return [(self.camera, _MOTION), (self.camera, _APPLES)] if self.camera is not None else []

    def _now(self) -> pd.Timestamp:
        return self.clock


def _motion(at: pd.Timestamp) -> pd.DataFrame:
    return pd.DataFrame({"camera.motion": [b"jpeg"]}, index=[at])


def test_motion_closes_the_shutter():
    protector = _Protector()
    protector._on_motion_detect(_motion(T0))
    assert protector.shutter.writes == [True]
    assert protector.is_closed()


def test_motion_while_closed_is_ignored_and_keeps_the_timer():
    protector = _Protector(delay=300)
    protector._on_motion_detect(_motion(T0))
    timer = protector._timer
    try:
        assert timer is not None
        # Past any cooldown armed at close time: the shutter is still closed, the camera sees only it.
        protector._on_motion_detect(_motion(T0 + _seconds(60)))
        assert protector.shutter.writes == [True]
        assert protector._timer is timer
    finally:
        timer.cancel()


def test_open_arms_cooldown_so_the_opening_movement_cannot_retrigger():
    protector = _Protector(cooldown=10)
    protector._on_motion_detect(_motion(T0))
    # Delay longer than cooldown, as deployed (10min / 30s)
    protector.clock = T0 + _seconds(60)
    protector.open()
    assert protector.shutter.writes == [True, False]
    assert not protector.is_closed()
    assert protector.is_in_cooldown()

    protector._on_motion_detect(_motion(T0 + _seconds(61)))
    assert protector.shutter.writes == [True, False]

    protector._on_motion_detect(_motion(T0 + _seconds(70)))
    assert protector.shutter.writes == [True, False, True]


def test_trigger_runs_the_same_cycle():
    protector = _Protector(cooldown=10)
    protector.trigger()
    assert protector.is_closed()
    protector.clock = T0 + _seconds(5)
    protector.open()
    assert protector.is_in_cooldown()
    protector.clock = T0 + _seconds(15)
    assert not protector.is_in_cooldown()


def test_consumer_channels_are_muted_while_closed():
    protector = _Protector(cooldown=10)
    protector._on_motion_detect(_motion(T0))
    assert protector.camera.muted == {"camera.motion", "camera.apples"}
    protector.clock = T0 + _seconds(60)
    protector.open()
    assert protector.camera.muted == set()
    assert protector.is_in_cooldown()


def test_cycle_runs_without_a_camera_connector():
    protector = _Protector()
    protector.camera = None
    protector._on_motion_detect(_motion(T0))
    protector.open()
    assert protector.shutter.writes == [True, False]


def _channel(key: str, stream: bool) -> SimpleNamespace:
    return SimpleNamespace(key=key, get=lambda attr, default=None: stream if attr == "stream" else default)


def test_consumers_are_all_stream_channels_but_the_raw_view():
    assert CameraProtector._is_consumer(_channel("motion", True))
    assert CameraProtector._is_consumer(_channel("apples", True))
    assert not CameraProtector._is_consumer(_channel("stream", True))
    assert not CameraProtector._is_consumer(_channel("frame", False))


def test_camera_is_muted_while_its_protection_is_closed():
    camera = object.__new__(Camera)
    camera.protection = SimpleNamespace(is_enabled=lambda: True, is_closed=lambda: True)
    assert camera.is_muted()
    camera.protection.is_closed = lambda: False
    assert not camera.is_muted()
    camera.protection = None
    assert not camera.is_muted()
