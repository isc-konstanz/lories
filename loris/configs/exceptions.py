# -*- coding: utf-8 -*-
"""
    loris.configs.exceptions
    ~~~~~~~~~~~~~~~~~~~~~~~~
    
    
"""
from loris import LocalResourceException, LocalResourceUnavailableException


class ConfigurationException(LocalResourceException):
    """
    Raise if a configuration is invalid.

    """
    pass


class ConfigurationUnavailableException(LocalResourceUnavailableException, ConfigurationException):
    """
    Raise if a configuration file can not be found.

    """
    pass
