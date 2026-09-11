# -*- coding: utf-8 -*-
"""
tests.test_camera_protection_integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Boots a headless application with a real camera from a conf directory and
drives the protector's consumer resolution through the real data access and
connector wiring. The unit tests fake that resolution, which hid a crash on
the box: iterating a data access yields channel ids, not channels.
"""

from __future__ import annotations

import pytest

_SETTINGS_CONF = """
name = "camera_protection_it"
action = "run"

[interface]
enabled = false
"""

_SYSTEM_CONF = """
key = "camera_it"
name = "Camera Protection Integration Test"
"""

_CAMERA_CONF = """
[channels]
stream = true
motion = true

[data.channels]
connector = "opencv"

[protection]
delay = "2s"
cooldown = "3s"

[protection.data.channels.state]
type = "bool"
connector = "dummy"

[connectors.opencv]
type = "OpenCV"
host = "127.0.0.1"
port = 554

[connectors.dummy]
type = "dummy"
"""


@pytest.fixture
def camera(tmp_path, monkeypatch):
    pytest.importorskip("cv2")
    lories = pytest.importorskip("lories")
    from lories.components.cameras import Camera

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "system.conf").write_text(_SYSTEM_CONF)
    (conf_dir / "camera.conf").write_text(_CAMERA_CONF)
    monkeypatch.chdir(tmp_path)

    app = lories.load("camera_protection_it")
    cameras = [component for component in app.components.values() if isinstance(component, Camera)]
    assert len(cameras) == 1, f"expected one camera, found {[c.id for c in app.components.values()]}"
    return cameras[0]


def test_protector_resolves_the_motion_channel_through_the_real_wiring(camera):
    from lories.components.cameras import Camera
    from lories.connectors.cameras import CameraConnector

    protector = camera.protection
    streams = protector._consumer_streams()
    assert [channel.key for _, channel in streams] == ["motion"]

    connector, motion = streams[0]
    assert isinstance(connector, CameraConnector)
    assert connector is motion.connector._connector
    assert not protector._is_consumer(camera.data.get(Camera.STREAM))
    assert not protector._is_consumer(camera.data.get(Camera.FRAME))


def test_mute_and_unmute_reach_the_camera_connector(camera):
    from lories.components.cameras import Camera

    protector = camera.protection
    motion = camera.data.get(Camera.MOTION)
    stream = camera.data.get(Camera.STREAM)
    connector = motion.connector._connector

    protector._mute_consumers()
    assert connector.is_muted(motion)
    assert not connector.is_muted(stream)

    protector._unmute_consumers()
    assert not connector.is_muted(motion)
