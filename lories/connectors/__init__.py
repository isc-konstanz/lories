# -*- coding: utf-8 -*-
"""
lories.connectors
~~~~~~~~~~~~~~~~~


"""

from .._core import (  # noqa: F401
    ConnectType,
)

from .errors import (  # noqa: F401
    ConnectorError,
    ConnectorUnavailableError,
    ConnectionError,
    DatabaseError,
    DatabaseUnavailableError,
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
from ..data.database import Database  # noqa: F401

from ..data import databases  # noqa: F401
from ..data.databases import Databases  # noqa: F401

import importlib
import logging
import sys

from lories.core.importing import inject_mock as _inject_mock

_logger = logging.getLogger(__name__)

CONNECTORS = [
    "virtual",
    "math",
    "csv",
    "sql",
    "influxdb1",
    "influxdb2",
    "tables",
    "cameras",
    "serial.sdi12",
    "serial.i2c",
    "modbus",
    "mqtt",
    "revpi",
    "entsoe",
    "opcua",
]

_unavailable: dict[str, str] = {}
_mocked: dict[str, str] = {}

for _connector in CONNECTORS:
    _attempts = 0
    _missing_dep = None
    while True:
        try:
            importlib.import_module(f".{_connector}", "lories.connectors")
            if _attempts > 0:
                _mocked[_connector] = _missing_dep
            break

        except ImportError as e:
            _missing_dep = e.name or str(e).split("'")[1]

            if _attempts > 0:
                _logger.warning("Connector '%s' unavailable (missing: %s)", _connector, _missing_dep)
                _unavailable[_connector] = _missing_dep
                break

            _logger.debug("Retrying '%s' with mock for missing dep '%s'", _connector, _missing_dep)
            _inject_mock(_missing_dep)
            sys.modules.pop(f"lories.connectors.{_connector}", None)
            _attempts += 1

for _connector, _dep in _mocked.items():
    _mod = sys.modules.get(f"lories.connectors.{_connector}")
    if _mod is None:
        continue
    _mod_name = f"lories.connectors.{_connector}"
    for _obj in vars(_mod).values():
        if isinstance(_obj, type) and _obj.__module__ == _mod_name:
            _obj.__available__ = False
            _obj.__import_error__ = f"'{_dep}' is not installed."

del importlib
