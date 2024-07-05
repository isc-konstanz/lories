# -*- coding: utf-8 -*-
"""
loris.connectors
~~~~~~~~~~~~~~~~


"""

from .connector import (  # noqa: F401
    ConnectorMeta,
    Connector,
    ConnectorException,
    ConnectionException,
)

from . import registry  # noqa: F401
from .registry import ConnectorRegistration, register  # noqa: F401

from . import context  # noqa: F401
from .context import ConnectorContext  # noqa: F401

from .csv import CsvConnector
registry.types[CsvConnector.TYPE] = ConnectorRegistration(CsvConnector, CsvConnector.TYPE)

try:
    from .mysql import MySqlConnector
    registry.types[MySqlConnector.TYPE] = ConnectorRegistration(MySqlConnector, MySqlConnector.TYPE)

except ImportError:
    pass
