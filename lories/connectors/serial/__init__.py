# -*- coding: utf-8 -*-
"""
lories.connectors.serial
~~~~~~~~~~~~~~~~~~~~~~~~


"""

import importlib

CONNECTORS = [
    "sdi12",
    "i2c",
]

for import_connector in CONNECTORS:
    try:
        importlib.import_module(f".{import_connector}", "lories.connectors.serial")

    except ModuleNotFoundError:
        # TODO: Implement meaningful logging here
        pass

del importlib
