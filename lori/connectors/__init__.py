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

from .database import (  # noqa: F401
    Database,
    DatabaseException,
    DatabaseUnavailableException,
)

import importlib

for import_connector in ["dummy", "csv", "sql", "influx", "tables", "camera", "modbus", "revpi", "influx"]:
    try:
        importlib.import_module(f".{import_connector}", "lori.connectors")

    except ModuleNotFoundError:
        # TODO: Implement meaningful logging here
        pass

del importlib
