# -*- coding: utf-8 -*-
"""
lori.data.converters.errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori.core.errors import ResourceError


class ConversionError(ResourceError):
    """
    Raise if a conversion failed

    """
