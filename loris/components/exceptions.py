# -*- coding: utf-8 -*-
"""
    loris.components.exceptions
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    
"""
from loris import LocalResourceException, LocalResourceUnavailableException


class ComponentException(LocalResourceException):
    """
    Raise if an error occurred accessing the component.

    """
    pass


class ComponentUnavailableException(LocalResourceUnavailableException, ComponentException):
    """
    Raise if an accessed component can not be found.

    """
    pass
