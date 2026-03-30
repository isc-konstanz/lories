# -*- coding: utf-8 -*-
"""
lories.components.tariff
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from . import tariff  # noqa: F401
from .tariff import (  # noqa: F401
    Tariff,
    register_tariff_type,
    registry,
)

from . import provider  # noqa: F401
from .provider import TariffProvider  # noqa: F401

import importlib
import logging

_logger = logging.getLogger(__name__)

for import_provider in ["static", "entsoe"]:
    try:
        importlib.import_module(f".{import_provider}", "lories.components.tariff")

    except ImportError as e:
        _logger.debug("Failed to load tariff provider '%s': %s", import_provider, e)

del importlib
