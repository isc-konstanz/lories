# -*- coding: utf-8 -*-
"""
lori.simulation.report
~~~~~~~~~~~~~~~~~~~~~~


"""

from . import core  # noqa: F401
from .core import (  # noqa: F401
    Report,
    ReportException,
    register_report_type,
    registry,
)

import importlib

for import_report in ["excel", "pdf"]:
    try:
        importlib.import_module(f".{import_report}", "lori.simulation.report")

    except ModuleNotFoundError:
        # TODO: Implement meaningful logging here
        pass

del importlib
