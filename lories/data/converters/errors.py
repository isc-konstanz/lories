# -*- coding: utf-8 -*-
"""
lories.data.converters.errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories.core.errors import ResourceError


class ConversionError(ResourceError):
    """
    Raise if a conversion failed

    """
