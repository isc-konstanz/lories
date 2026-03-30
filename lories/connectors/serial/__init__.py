# -*- coding: utf-8 -*-
"""
lories.connectors.serial
~~~~~~~~~~~~~~~~~~~~~~~~


"""

import importlib
import logging

_logger = logging.getLogger(__name__)

CONNECTORS = [
    "sdi12",
    "i2c",
]

for import_connector in CONNECTORS:
    try:
        importlib.import_module(f".{import_connector}", "lories.connectors.serial")

    except ImportError as e:
        _logger.debug("Failed to load serial connector '%s': %s", import_connector, e)

del importlib
