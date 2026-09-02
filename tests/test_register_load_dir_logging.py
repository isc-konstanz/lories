# -*- coding: utf-8 -*-
"""
lories.tests.test_register_load_dir_logging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``_RegistratorContext._load_from_dir`` used to swallow every unresolvable
``.conf`` in a conf directory without a log line, so a typo'd ``type =`` was
indistinguishable from a deliberate non-registrator file (lories-frictions
issue 04). An explicit ``type`` key that fails to resolve now logs a WARNING;
a stem-only skip logs DEBUG.

The application configures logging itself (``basicConfig(force=True)`` at
INFO, or ``fileConfig`` when a ``logging.conf`` exists), which strips pytest's
caplog handler -- so these tests assert on the emitted stdout lines instead.
"""

from __future__ import annotations

import sys
from argparse import ArgumentParser

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

_MACHINES = """
name = "Machines"
type = "component"
"""

_ROGUE = """
name = "Rogue"
type = "does_not_exist_xyz"
"""

_NOTES = """
title = "Not a component at all"
"""

_LOGGING_DEBUG = """
[loggers]
keys=root

[handlers]
keys=console

[formatters]
keys=plain

[logger_root]
level=DEBUG
handlers=console

[handler_console]
class=StreamHandler
level=DEBUG
formatter=plain
args=(sys.stdout,)

[formatter_plain]
format=%(name)s - %(levelname)s - %(message)s
"""


def _write_project(tmp_path, logging_conf: bool = False) -> None:
    conf = tmp_path / "conf"
    conf.mkdir(parents=True)
    (tmp_path / "settings.conf").write_text(_SETTINGS)
    (conf / "system.conf").write_text(_SYSTEM)
    (conf / "machines.conf").write_text(_MACHINES)
    (conf / "rogue.conf").write_text(_ROGUE)
    (conf / "notes.conf").write_text(_NOTES)
    if logging_conf:
        (conf / "logging.conf").write_text(_LOGGING_DEBUG)


def _load_system(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["probe", "-c", str(tmp_path / "conf"), "-d", str(tmp_path), "run"])
    app = Application.load("probe", parser=ArgumentParser())
    return next(iter(app.components.values()))


def test_unresolved_explicit_type_logs_warning(tmp_path, monkeypatch, capsys):
    _write_project(tmp_path)
    system = _load_system(tmp_path, monkeypatch)
    out = capsys.readouterr().out

    # The boot survives the rogue file and still loads the well-typed sibling
    assert "machines" in [c.key for c in system.components.values()]

    assert "Skipping configuration file 'rogue.conf' with type 'does_not_exist_xyz'" in out
    # At the default INFO level the type-less file is skipped without any line
    assert "notes.conf" not in out


def test_typeless_file_skip_logs_debug(tmp_path, monkeypatch, capsys):
    _write_project(tmp_path, logging_conf=True)
    _load_system(tmp_path, monkeypatch)
    out = capsys.readouterr().out

    notes_lines = [line for line in out.splitlines() if "notes.conf" in line]
    assert notes_lines, "expected a DEBUG skip line naming notes.conf"
    assert all("DEBUG" in line for line in notes_lines)

    rogue_lines = [line for line in out.splitlines() if "rogue.conf" in line]
    assert any("WARNING" in line and "does_not_exist_xyz" in line for line in rogue_lines)
