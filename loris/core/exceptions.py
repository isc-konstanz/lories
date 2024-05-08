# -*- coding: utf-8 -*-
"""
    loris.core.exceptions
    ~~~~~~~~~~~~~~~~~~~~~
    
    
"""


class LocalResourceException(Exception):
    """
    Raise if an error occurred accessing a local resource.

    """
    pass


class ComponentException(LocalResourceException):
    """
    Raise if an error occurred accessing the component.

    """
    pass


class ComponentUnavailableException(ComponentException):
    """
    Raise if an accessed component can not be found.

    """
    pass


class ConnectorException(ComponentException):
    """
    Raise if an error occurred accessing the connector.

    """
    pass


class ConnectionException(ConnectorException, IOError):
    """
    Raise if an error occurred with the connection.

    """
    pass
