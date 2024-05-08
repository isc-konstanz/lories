# -*- coding: utf-8 -*-
"""
    loris.exceptions
    ~~~~~~~~~~~~~~~~
    
    
"""


class LocalResourceException(Exception):
    """
    Raise if an error occurred accessing a local resource.

    """
    pass


class LocalResourceUnavailableException(LocalResourceException):
    """
    Raise if an accessed local resource can not be found.

    """
    pass
