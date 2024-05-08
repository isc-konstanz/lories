# -*- coding: utf-8 -*-
"""
    loris.core.configs.exceptions
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    
"""
from loris.core import LocalResourceException


class ConfigurationException(Exception):
    """
    Raise if a configuration is invalid.

    """
    pass


class ConfigurationUnavailableException(ConfigurationException):
    """
    Raise if a configuration file can not be found.

    """
    pass
