# -*- coding: utf-8 -*-
"""
lories.tests.test_startup_log_noise
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Startup used to print one bare "Connector '...' unavailable" line per missing optional
dependency from every process that imported lories (the camera stream spawns a Manager
and a pool worker, each re-importing the package), plus Flask's server banner and a
doubled "Dash is running" line. The registry now only records; the application logs a
single INFO summary once logging is configured.

The application configures logging itself (``basicConfig(force=True)``), which strips
pytest's caplog handler, so the boot tests assert on stdout.
"""

from __future__ import annotations

import importlib
import os
import sys
from argparse import ArgumentParser

import pytest

import lories.connectors as connectors
from lories.application import Application

_SETTINGS = """
[data]
freq = "1min"
"""

_SYSTEM = """
key = "probe"
name = "Probe System"

[location]
latitude = 47.67
longitude = 9.15
timezone = "Europe/Berlin"
"""


def _boot(tmp_path, monkeypatch) -> None:
    conf = tmp_path / "conf"
    conf.mkdir(parents=True)
    (tmp_path / "settings.conf").write_text(_SETTINGS)
    (conf / "system.conf").write_text(_SYSTEM)
    monkeypatch.setattr(sys, "argv", ["probe", "-c", str(conf), "-d", str(tmp_path), "run"])
    Application.load("probe", parser=ArgumentParser())


def test_unavailable_merges_hard_and_mocked_records(monkeypatch):
    monkeypatch.setattr(connectors, "_unavailable", {"hard": "dep_a"})
    monkeypatch.setattr(connectors, "_mocked", {"soft": "dep_b, dep_c"})
    assert connectors.unavailable() == {"hard": "dep_a", "soft": "dep_b, dep_c"}


def test_application_logs_one_summary_line(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(connectors, "_unavailable", {"probe_hard": "probe_dep"})
    monkeypatch.setattr(connectors, "_mocked", {"probe_soft": "dep_x, dep_y"})
    _boot(tmp_path, monkeypatch)
    out = capsys.readouterr().out

    lines = [line for line in out.splitlines() if "Optional connectors not installed:" in line]
    assert len(lines) == 1, out
    assert "probe_hard (probe_dep), probe_soft (dep_x, dep_y)" in lines[0]
    # Formatted through the configured handler (timestamp - logger - message), not a bare line
    assert " - " in lines[0].split("Optional connectors")[0], lines[0]
    assert "unavailable (missing" not in out


def test_application_silent_when_nothing_is_missing(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(connectors, "_unavailable", {})
    monkeypatch.setattr(connectors, "_mocked", {})
    _boot(tmp_path, monkeypatch)
    out = capsys.readouterr().out
    assert "Optional connectors not installed" not in out


def test_opencv_quiets_ffmpeg_decoder_log():
    pytest.importorskip("cv2")
    importlib.import_module("lories.connectors.cameras.opencv")
    assert os.environ.get("OPENCV_FFMPEG_LOGLEVEL") == "8"


def test_flask_server_banner_is_silenced(capsys):
    pytest.importorskip("dash")
    importlib.import_module("lories.application.view.interface")
    import flask.cli

    flask.cli.show_server_banner(False, "probe")
    assert capsys.readouterr().out == ""
