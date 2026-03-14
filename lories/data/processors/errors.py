# -*- coding: utf-8 -*-
"""
lories.data.processors.errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories.core.errors import ResourceError


class ProcessingError(ResourceError):
    """
    Raise if processing a value failed

    """
