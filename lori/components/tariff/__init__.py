# -*- coding: utf-8 -*-
"""
lori.components.tariff
~~~~~~~~~~~~~~~~~~~~~~


"""

from . import core  # noqa: F401
from .core import (  # noqa: F401
    Tariff,
    TariffException,
    TariffUnavailableException,
    register_tariff_type,
    registry,
)

from . import provider  # noqa: F401
from .provider import TariffProvider  # noqa: F401

import importlib

for import_provider in ["static", "entsoe"]:
    try:
        importlib.import_module(f".{import_provider}", "lori.components.tariff")

    except ModuleNotFoundError:
        # TODO: Implement meaningful logging here
        pass

del importlib
