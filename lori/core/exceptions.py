# -*- coding: utf-8 -*-
"""
lori.exceptions
~~~~~~~~~~~~~~~


"""


class ResourceException(Exception):
    """
    Raise if an error occurred accessing a local resource.

    """


class ResourceUnavailableException(ResourceException):
    """
    Raise if an accessed local resource can not be found.

    """
