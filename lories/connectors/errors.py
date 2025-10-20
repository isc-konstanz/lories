# -*- coding: utf-8 -*-
"""
lories.connectors.errors
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories._core._connector import Connector  # noqa
from lories.core import ResourceError, ResourceUnavailableError


class ConnectorError(ResourceError):
    """
    Raise if an error occurred accessing the connector.

    """

    # noinspection PyArgumentList
    def __init__(self, connector: Connector, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.connector = connector


class ConnectorUnavailableError(ResourceUnavailableError, ConnectorError):
    """
    Raise if an accessed connector can not be found.

    """


# noinspection PyShadowingBuiltins
class ConnectionError(ConnectorError, ConnectionError):
    """
    Raise if an error occurred with the connection.

    """


class DatabaseException(ConnectorError):
    """
    Raise if an error occurred accessing the database.

    """


class DatabaseUnavailableException(ConnectorUnavailableError):
    """
    Raise if an accessed database can not be found.

    """
