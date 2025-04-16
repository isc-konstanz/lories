# -*- coding: utf-8 -*-
"""
lori.application
~~~~~~~~~~~~~~~~


"""

from .interface import (  # noqa: F401
    Interface,
    InterfaceException,
    register_interface_type,
)

from .main import Application  # noqa: F401

import importlib

for import_interface in ["view"]:
    try:
        importlib.import_module(f".{import_interface}", "lori.application")

    except ModuleNotFoundError:
        # TODO: Implement meaningful logging here
        pass

del importlib
