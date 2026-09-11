# -*- coding: utf-8 -*-
"""
tests.test_camera_protection_optin
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The protection shutter is opt-in. A camera that never configured one must not end up with an
enabled protector: the protector demands a ``motion`` channel at activation, so an implicit one
takes down every plain camera. The section may be declared inline or as a ``camera.d`` member,
and both have to reach the same decision.
"""

from __future__ import annotations

import pytest

from lories.components.cameras.protection import CameraProtector

_SETTINGS_CONF = """
name = "camera_protection_optin"
action = "run"

[interface]
enabled = false
"""

_SYSTEM_CONF = """
key = "camera_optin"
name = "Camera Protection Opt-In Test"
"""

_CONNECTORS_CONF = """
[connectors.opencv]
type = "OpenCV"
host = "127.0.0.1"
port = 554

[connectors.dummy]
type = "dummy"
"""

_PROTECTION_CONF = """
delay = "2s"
cooldown = "3s"

[data.channels.state]
type = "bool"
connector = "dummy"
"""


def _boot(tmp_path, monkeypatch, camera_conf: str, member_conf: str = None):
    pytest.importorskip("cv2")
    lories = pytest.importorskip("lories")
    from lories.components.cameras import Camera

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "system.conf").write_text(_SYSTEM_CONF)
    (conf_dir / "camera.conf").write_text(camera_conf + _CONNECTORS_CONF)
    if member_conf is not None:
        member_dir = conf_dir / "camera.d"
        member_dir.mkdir()
        (member_dir / "protection.conf").write_text(member_conf)
    monkeypatch.chdir(tmp_path)

    app = lories.load("camera_protection_optin")
    cameras = [component for component in app.components.values() if isinstance(component, Camera)]
    assert len(cameras) == 1, f"expected one camera, found {[c.id for c in app.components.values()]}"
    return cameras[0]


_PLAIN_CAMERA = """
[channels]
stream = false
motion = false

[data.channels]
connector = "opencv"

"""

_MOTION_CAMERA = """
[channels]
stream = true
motion = true

[data.channels]
connector = "opencv"

"""

_DISABLED_PROTECTION = (
    _MOTION_CAMERA
    + """
[protection]
enabled = false
delay = "2s"

"""
)

_INLINE_PROTECTION = (
    _MOTION_CAMERA
    + """
[protection]
delay = "2s"
cooldown = "3s"

[protection.data.channels.state]
type = "bool"
connector = "dummy"

"""
)


def test_camera_without_a_protection_section_has_no_protector(tmp_path, monkeypatch):
    camera = _boot(tmp_path, monkeypatch, _PLAIN_CAMERA)
    assert camera.protection is None
    assert not camera.has_protection()
    assert CameraProtector.TYPE not in [component.key for component in camera.components.values()]


def test_disabled_protection_section_creates_no_protector(tmp_path, monkeypatch):
    camera = _boot(tmp_path, monkeypatch, _DISABLED_PROTECTION)
    assert camera.protection is None
    assert not camera.has_protection()


def test_inline_protection_section_creates_a_protector(tmp_path, monkeypatch):
    camera = _boot(tmp_path, monkeypatch, _INLINE_PROTECTION)
    assert camera.protection is not None
    assert camera.has_protection()


def test_protection_member_file_creates_a_protector(tmp_path, monkeypatch):
    camera = _boot(tmp_path, monkeypatch, _MOTION_CAMERA, member_conf=_PROTECTION_CONF)
    assert camera.protection is not None
    assert camera.has_protection()
