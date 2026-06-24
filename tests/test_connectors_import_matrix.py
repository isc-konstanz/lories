# -*- coding: utf-8 -*-
"""Seam 4 — optional-dep import matrix; pins the B4 contract by inspecting bookkeeping, not reloading (issue 06)."""

from __future__ import annotations

import sys

import pytest

# Connectors with no optional third-party dependency — always available.
DEPENDENCY_FREE = ("virtual", "math", "csv")


@pytest.fixture(scope="module")
def connectors():
    return pytest.importorskip("lories.connectors")


def test_partial_install_still_imports(connectors):
    assert isinstance(connectors.CONNECTORS, list) and connectors.CONNECTORS
    assert isinstance(connectors._unavailable, dict)
    assert isinstance(connectors._mocked, dict)


def test_config_system_still_parses(connectors, write_conf):
    # A partial connector install must not break config parsing.
    configs = write_conf('type = "virtual"\nenabled = true\n')
    assert configs.get("type") == "virtual"
    assert configs.enabled is True


def test_every_connector_imported_or_recorded(connectors):
    """Every connector is either imported (dep present) or recorded unavailable/mocked (dep absent)."""
    recorded = set(connectors._unavailable) | set(connectors._mocked)
    for name in connectors.CONNECTORS:
        imported = sys.modules.get(f"lories.connectors.{name}") is not None
        assert imported or name in recorded, f"{name} is neither imported nor recorded unavailable/mocked"


def test_dependency_free_connectors_are_real(connectors):
    """No-dep connectors are genuine modules (never shadowed by a mock)."""
    from lories.core.importing import MockModule

    for name in DEPENDENCY_FREE:
        assert name not in connectors._unavailable
        assert name not in connectors._mocked
        mod = sys.modules.get(f"lories.connectors.{name}")
        assert mod is not None and not isinstance(mod, MockModule)


def test_mocked_connectors_fail_clearly(connectors):
    """A mocked connector's classes are flagged with a clear 'not installed' reason (empty on a full install)."""
    for name in connectors._mocked:
        mod = sys.modules.get(f"lories.connectors.{name}")
        if mod is None:
            continue
        flagged = [
            obj for obj in vars(mod).values() if isinstance(obj, type) and getattr(obj, "__available__", True) is False
        ]
        assert flagged, f"{name} loaded against a mock but no class is flagged unavailable"
        assert all("not installed" in getattr(obj, "__import_error__", "") for obj in flagged)


def test_mock_module_contract_and_no_leak():
    """Mock attr access succeeds, instantiation errors clearly, and the injected module doesn't persist."""
    from lories.core.importing import MockModule, inject_mock

    fake = "lories_eval_fake_dep_xyz"
    assert fake not in sys.modules
    try:
        inject_mock(fake)
        mod = sys.modules.get(fake)
        assert isinstance(mod, MockModule)

        fabricated = mod.SomeClient  # attribute access succeeds (module loads cleanly)
        assert isinstance(fabricated, type)
        assert fabricated.__bases__ == (object,)  # independent type, masquerades as nothing real

        with pytest.raises(RuntimeError, match="not installed"):
            fabricated()  # ...but instantiation fails clearly
    finally:
        sys.modules.pop(fake, None)

    assert fake not in sys.modules  # no persistent fake module leak
