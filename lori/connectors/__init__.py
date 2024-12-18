# -*- coding: utf-8 -*-
"""
lori.connectors
~~~~~~~~~~~~~~~


"""

from .connector import (  # noqa: F401
    ConnectorMeta,
    Connector,
    ConnectorException,
    ConnectorUnavailableException,
    ConnectionException,
)

from . import context  # noqa: F401
from .context import (  # noqa: F401
    ConnectorContext,
    register_connector_type,
    registry,
)

from .access import ConnectorAccess  # noqa: F401

from .dummy import DummyConnector  # noqa: F401
from .csv import CsvConnector  # noqa: F401

try:
    from .sql import SqlConnector  # noqa: F401

except ModuleNotFoundError:
    pass
