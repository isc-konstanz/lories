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

from . import context  # noqa: F401
from .context import (  # noqa: F401
    ConnectorContext,
    register_connector_type,
    registry,
)

from .access import ConnectorAccess  # noqa: F401

from .csv import CsvConnector

try:
    from .mysql import MySqlConnector

except ImportError:
    pass
