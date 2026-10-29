# -*- coding: utf-8 -*-
"""
tests.test_components_context_deactivate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An already inactive component must not stop the deactivation of the others.
"""

from __future__ import annotations

import signal

import pytest

_SETTINGS_CONF = 'name = "components_deactivate_test"\naction = "run"\n\n[interface]\nenabled = false\n'
_COMPONENT_CONF = 'type = "component"\n'


@pytest.fixture
def app(tmp_path, monkeypatch):
    lories = pytest.importorskip("lories")

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "first.conf").write_text(_COMPONENT_CONF)
    (conf_dir / "second.conf").write_text(_COMPONENT_CONF)
    monkeypatch.chdir(tmp_path)

    handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    application = lories.load("components_deactivate_test")
    yield application
    application.deactivate()
    for s, handler in handlers.items():
        signal.signal(s, handler)


@pytest.mark.parametrize("index", [0, 1])
def test_deactivate_skips_inactive_component(app, index):
    app.activate()
    components = list(app.components.values())
    assert len(components) == 2
    assert all(c.is_active() for c in components)

    inactive = components[index]
    app.components.deactivate(lambda c: c.id == inactive.id)
    assert not inactive.is_active()

    app.components.deactivate()
    assert not any(c.is_active() for c in components)
