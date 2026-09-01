# -*- coding: utf-8 -*-
"""
tests.test_camera_min_size
~~~~~~~~~~~~~~~~~~~~~~~~~~

A camera configured with ``min_size`` must drop captured frames below that byte
count without touching the channel: nothing logged, no listener fired, last good
frame kept. Without ``min_size`` the frame channel stays exactly as before.
"""

from __future__ import annotations

import logging
import os

import pytest

import pandas as pd
from lories.data.processors import Processor, SizeFilter

T0 = pd.Timestamp("2026-01-01T12:00:00Z")

_SETTINGS_CONF = 'name = "camera_min_size"\naction = "run"\n\n[interface]\nenabled = false\n'

# The stock Camera requires the [channels] member: its ParameterGroup binds to None
# when the section is absent and configure crashes on .get(). Camera-level keys must
# come before the header, anything after it lands in the channels table.
_CHANNELS_CONF = "\n[channels]\nstream = false\n"


def test_payload_below_min_size_is_skipped():
    size = SizeFilter(key="size", min_size=100)
    assert size(T0, b"x" * 99) is Processor.SKIP


def test_payload_at_or_above_min_size_passes():
    size = SizeFilter(key="size", min_size=100)
    assert size(T0, b"x" * 100) == b"x" * 100
    assert size(T0, b"x" * 101) == b"x" * 101


def test_non_bytes_pass_through():
    size = SizeFilter(key="size", min_size=100)
    assert size(T0, 3.5) == 3.5


def test_skip_is_logged_with_sizes(caplog):
    size = SizeFilter(key="size", min_size=100)
    with caplog.at_level(logging.INFO, logger="lories.data.processors.size"):
        size(T0, b"x" * 10)
    assert "10 bytes" in caplog.text
    assert "100" in caplog.text


def _boot_camera(tmp_path, camera_conf: str):
    """Boot a real lories ``Application`` with one loose camera and return the camera."""
    from lories.application import Settings
    from lories.application.main import Application
    from lories.components.cameras.camera import Camera

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir(exist_ok=True)
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "camera.conf").write_text(camera_conf + _CHANNELS_CONF)

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        settings = Settings("camera_min_size")
        app = Application(settings)
        app.configure(settings)
    finally:
        os.chdir(cwd)
    cameras = app._components.get_all(Camera)
    assert len(cameras) == 1
    return cameras[0]


def test_camera_without_min_size_has_no_frame_processor(tmp_path):
    from lories.components.cameras.camera import Camera

    camera = _boot_camera(tmp_path, "")
    frame = camera.data.get(Camera.FRAME)
    assert len(frame.processors) == 0

    frame.value = b"x" * 10
    assert frame.value == b"x" * 10


def test_camera_min_size_drops_small_frames_and_keeps_last_good(tmp_path):
    from lories.components.cameras.camera import Camera

    camera = _boot_camera(tmp_path, "min_size = 1000\n")
    frame = camera.data.get(Camera.FRAME)
    assert f"{frame.id}.size" in frame.processors

    frame.value = b"x" * 999
    assert frame.value is None
    assert pd.isna(frame.timestamp)

    good = b"x" * 1000
    frame.value = good
    assert frame.value == good
    timestamp = frame.timestamp

    frame.value = b"x" * 10
    assert frame.value == good
    assert frame.timestamp == timestamp


def test_camera_min_size_must_not_be_negative(tmp_path):
    from lories.core import ConfigurationError

    with pytest.raises(ConfigurationError, match="min_size"):
        _boot_camera(tmp_path, "min_size = -1\n")
