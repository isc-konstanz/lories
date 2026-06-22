# -*- coding: utf-8 -*-
"""
lories.core.configs.toml
~~~~~~~~~~~~~~~~~~~~~~~~


"""

import re
from typing import Any, Mapping

try:
    import tomllib as toml
except ModuleNotFoundError:
    import tomli as toml


def load_toml(conf_path: str) -> Mapping[str, Any]:
    # TOML mandates UTF-8; read it explicitly so configs with non-ASCII chars
    # (e.g. ``cm⁻¹``, ``Sₑ``) load identically on Windows (default cp1252) and
    # Linux instead of raising ``UnicodeDecodeError`` on the former.
    with open(conf_path, mode="r", encoding="utf-8") as conf_file:
        conf_string = conf_file.read()
        conf_string = re.sub(r"^;+", "#", conf_string, flags=re.MULTILINE)

        return toml.loads(conf_string)
