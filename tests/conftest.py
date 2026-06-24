# -*- coding: utf-8 -*-
"""Shared fixtures for the parameters / descriptor-layer test suite."""
from __future__ import annotations

import pytest

from lories.core.configs.configurations import Configurations


@pytest.fixture
def write_conf(tmp_path):
    """Write TOML to a flat .conf file and load it via Configurations.load(flat=True)."""

    def _write(toml_text: str, name: str = "test.conf") -> Configurations:
        (tmp_path / name).write_text(toml_text)
        return Configurations.load(name, data_dir=str(tmp_path), flat=True)

    return _write
