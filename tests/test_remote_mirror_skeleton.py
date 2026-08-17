# -*- coding: utf-8 -*-
"""Unit tests for the ``remote_mirror`` component skeleton (config resolution only, no DB/network)."""

from __future__ import annotations

import os
import tempfile

SETTINGS_CONF = """\
name = "remote_mirror_test"
action = "run"

[interface]
enabled = false

[components.remote_mirror_full]
type = "remote_mirror"
source = "remote_db"
target = "local_db"
mode = "push"
full = false
force = true
slice = "H"
freq = "H"
interval = 15
offset = 5

[components.remote_mirror_defaults]
type = "remote_mirror"
source = "remote_db_only"
"""


def _build_application(tmp_dir: str):
    """Boot a real ``Application`` from a temp ``settings.conf`` declaring two ``remote_mirror`` components."""
    from lories.application import Settings
    from lories.application.main import Application

    conf_dir = os.path.join(tmp_dir, "conf")
    data_dir = os.path.join(tmp_dir, "data")
    os.makedirs(conf_dir)
    os.makedirs(data_dir)

    with open(os.path.join(conf_dir, "settings.conf"), "w") as file:
        file.write(SETTINGS_CONF)

    cwd = os.getcwd()
    os.chdir(tmp_dir)
    try:
        settings = Settings("remote_mirror_test")
        app = Application(settings)
        app.configure(settings)
    finally:
        os.chdir(cwd)
    return app


def test_remote_mirror_config_surface_resolves():
    from lories.components import RemoteMirror

    tmp_dir = tempfile.mkdtemp(prefix="remote_mirror_test_")
    app = _build_application(tmp_dir)

    full = app.components["remote_mirror_full"]
    assert isinstance(full, RemoteMirror)
    assert full.is_configured()
    assert full.source == "remote_db"
    assert full.target == "local_db"
    assert full.mode == "push"
    assert full.full is False
    assert full.force is True
    assert full.slice == "H"
    assert full.freq == "H"
    assert full.interval == 15
    assert full.offset == 5

    defaults = app.components["remote_mirror_defaults"]
    assert isinstance(defaults, RemoteMirror)
    assert defaults.is_configured()
    assert defaults.source == "remote_db_only"
    assert defaults.target is None
    assert defaults.mode == "pull"
    assert defaults.full is True
    assert defaults.force is False
    assert defaults.slice == "D"
    assert defaults.freq == "D"
    assert defaults.interval == 60
    assert defaults.offset == 0


def test_activate_starts_and_deactivate_stops_scheduler():
    from lories.components import RemoteMirror

    app = _build_application(tempfile.mkdtemp(prefix="remote_mirror_sched_"))

    mirror = app.components["remote_mirror_defaults"]
    assert isinstance(mirror, RemoteMirror)
    # No-op the copy: this test asserts only that the scheduler lifecycle is tied to
    # activate/deactivate (the default 60-minute interval never fires within the test).
    mirror._mirror_once = lambda: None

    mirror.activate()
    try:
        assert mirror._scheduler is not None
        assert mirror._scheduler.is_running()
    finally:
        mirror.deactivate()

    assert mirror._scheduler is None
