# -*- coding: utf-8 -*-
"""
lori.core.configs.errors
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori.core.errors import ResourceError, ResourceUnavailableError


class ConfigurationError(ResourceError):
    """
    Raise if a configuration is invalid.

    """


class ConfigurationUnavailableError(ResourceUnavailableError, ConfigurationError):
    """
    Raise if a configuration file can not be found.

    """
