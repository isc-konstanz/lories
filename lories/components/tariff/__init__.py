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
