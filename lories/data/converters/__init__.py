# -*- coding: utf-8 -*-
"""
lories.data.converters
~~~~~~~~~~~~~~~~~~~~~~


"""

from .errors import ConversionError  # noqa: F401

from .converter import Converter  # noqa: F401

from . import access  # noqa: F401
from .access import ConverterAccess  # noqa: F401

from . import context  # noqa: F401
from .context import (  # noqa: F401
    ConverterContext,
    register_converter_type,
    registry,
)

from . import io  # noqa: F401

import importlib

CONVERTERS = [
    "json",
]

for import_converter in CONVERTERS:
    try:
        importlib.import_module(f".{import_converter}", "lories.data.converters")

    except ModuleNotFoundError:
        # TODO: Implement meaningful logging here
        pass

del importlib
