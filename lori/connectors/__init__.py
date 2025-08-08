# -*- coding: utf-8 -*-
"""
lori.connectors
~~~~~~~~~~~~~~~


"""

from .core import (  # noqa: F401
    _Connector,
    ConnectorException,
    ConnectorUnavailableException,
    ConnectionException,
)

from . import access  # noqa: F401
from .access import ConnectorAccess  # noqa: F401

from . import context  # noqa: F401
from .context import (  # noqa: F401
    ConnectorContext,
    register_connector_type,
    registry,
)

from . import connector  # noqa: F401
from .connector import Connector  # noqa: F401

from ..data import database  # noqa: F401
from ..data.database import (  # noqa: F401
    Database,
    DatabaseException,
    DatabaseUnavailableException,
)

from ..data import databases  # noqa: F401
from ..data.databases import Databases  # noqa: F401

import importlib

for import_connector in ["virtual", "csv", "sql", "influx", "tables", "cameras", "modbus", "secsgem", "revpi", "entsoe"]:
    try:
        importlib.import_module(f".{import_connector}", "lori.connectors")

    except ModuleNotFoundError:
        # TODO: Implement meaningful logging here
        pass

del importlib
