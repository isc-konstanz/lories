# -*- coding: utf-8 -*-
"""
lori.core.configs.toml
~~~~~~~~~~~~~~~~~~~~~~


"""

from typing import Any, Mapping

try:
    import tomllib as toml
except ModuleNotFoundError:
    import tomli as toml


def load_toml(conf_path: str) -> Mapping[str, Any]:
    with open(conf_path, mode="rb") as conf_file:
        return toml.load(conf_file)
