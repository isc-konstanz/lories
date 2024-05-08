# -*- coding: utf-8 -*-
"""
    loris.connectors.exceptions
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    
"""
from loris import LocalResourceException


class ConnectorException(LocalResourceException):
    """
    Raise if an error occurred accessing the connector.

    """
    pass


class ConnectionException(ConnectorException, IOError):
    """
    Raise if an error occurred with the connection.

    """
    pass
