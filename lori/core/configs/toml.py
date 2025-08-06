# -*- coding: utf-8 -*-
"""
lori.core.configs.toml
~~~~~~~~~~~~~~~~~~~~~~


"""

import re
from typing import Any, Mapping

try:
    import tomllib as toml
except ModuleNotFoundError:
    import tomli as toml


def load_toml(conf_path: str) -> Mapping[str, Any]:
    with open(conf_path, mode="r") as conf_file:
        conf_string = conf_file.read()
        conf_string = re.sub(r"^;+", "#", conf_string, flags=re.MULTILINE)

        return toml.loads(conf_string)
