# -*- coding: utf-8 -*-
"""
lories.core.errors
~~~~~~~~~~~~~~~~~~


"""


class ResourceError(Exception):
    """
    Raise if an error occurred accessing a local resource.

    """


class ResourceUnavailableError(ResourceError):
    """
    Raise if an accessed local resource can not be found.

    """
