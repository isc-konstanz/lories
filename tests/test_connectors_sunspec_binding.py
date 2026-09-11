# -*- coding: utf-8 -*-
"""
tests.test_connectors_sunspec_binding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unit tests for the ``SunSpecBinding`` mixin: ``_bind`` emits a ``linear`` converter only for a
``(point, scale)`` ``POINTS`` entry, the ``__init_subclass__`` guard rejects a mixin used
without a ``BindableComponent`` base or listed after it in the MRO, and the mixin module
itself stays importable -- yielding the real class, not a mock -- even with ``sunspec2``
blocked, since it carries no dependency on the ``sunspec2`` package.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any, Dict

import pytest

from lories.components.binding import BindableComponent
from lories.connectors.sunspec import SunSpecBinding
from lories.core import Constant

POWER = Constant(float, "power", "Power", "W", context="sunspecbindtest")
ENERGY = Constant(float, "energy", "Energy", "Wh", context="sunspecbindtest")
UNBOUND = Constant(float, "unbound", "Unbound", "", context="sunspecbindtest")


class _StubDevice(SunSpecBinding, BindableComponent):
    MODELS = [103]
    DEFAULT_MODEL = 103
    POINTS: Dict[Constant, Any] = {
        POWER: ("W", 0.1),
        ENERGY: "WH",
    }


def _bind_configured(constant: Constant, device: int = 1, model: int = 103, instance: int = 1) -> Dict[str, Any]:
    stub = _StubDevice.__new__(_StubDevice)
    stub.device = device
    stub.model = model
    stub.instance = instance
    stub._connector_id = "sunspec"
    return stub._bind(constant)


def test_bind_emits_linear_converter_only_for_tuple_points():
    power = _bind_configured(POWER)
    assert power["point"] == "W"
    assert power["converter"] == {"type": "linear", "scale": 0.1}
    assert power["device"] == 1
    assert power["model"] == 103
    assert power["instance"] == 1
    assert power["connector"] == "sunspec"

    energy = _bind_configured(ENERGY)
    assert energy["point"] == "WH"
    assert "converter" not in energy


def test_bind_returns_empty_for_unbound_constant():
    assert _bind_configured(UNBOUND) == {}


def test_mixin_requires_a_bindable_component_base():
    with pytest.raises(TypeError, match="BindableComponent"):

        class Orphan(SunSpecBinding):
            pass


def test_mixin_must_precede_the_device_in_the_mro():
    with pytest.raises(TypeError, match="must list SunSpecBinding"):

        class Backwards(BindableComponent, SunSpecBinding):
            pass


# ---------------------------------------------------------------- sunspec2-blocked import probe

_BLOCKER_PREAMBLE = """
import importlib.abc
import sys

BLOCKED = {blocked!r}


class Blocker(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        top = name.split(".")[0]
        if top in BLOCKED and top not in sys.modules:
            raise ModuleNotFoundError(f"No module named '{{name}}'", name=name)
        return None


sys.meta_path.insert(0, Blocker())

import lories  # noqa: F401
from lories.connectors.sunspec import SunSpecBinding
"""


def _run_probe(tmp_path, blocked, body: str):
    repo_root = Path(__file__).resolve().parents[1]
    script = tmp_path / "probe.py"
    source = _BLOCKER_PREAMBLE.format(blocked=tuple(blocked)) + textwrap.dedent(body)
    script.write_text(source, encoding="utf-8")
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(p for p in (str(repo_root), env.get("PYTHONPATH")) if p)
    result = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, env=env, cwd=repo_root, timeout=120
    )
    assert result.returncode == 0, f"probe failed:\n{result.stdout}\n{result.stderr}"
    return result


def test_sunspec2_blocked_import_still_yields_the_real_binding_class(tmp_path):
    _run_probe(
        tmp_path,
        ["sunspec2"],
        """
        assert hasattr(SunSpecBinding, "_bind"), "SunSpecBinding is not the real class"
        assert SunSpecBinding.__module__ == "lories.connectors.sunspec.binding"

        from lories.connectors import registry as connector_registry

        assert "sunspec" in connector_registry.get_types()
        registration = connector_registry.from_type("sunspec")
        assert registration.available is False
        """,
    )
