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
from lories.core.importing import is_mocked as _is_mocked

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
    "openems",
]

_unavailable: dict[str, str] = {}
_mocked: dict[str, str] = {}
_deps_by_owner: dict[str, list[str]] = {}


def _import_origin(error):
    # Innermost lories frame in the traceback = the module whose import statement failed.
    origin = None
    tb = error.__traceback__
    while tb is not None:
        name = tb.tb_frame.f_globals.get("__name__")
        if isinstance(name, str) and (name == "lories" or name.startswith("lories.")):
            origin = name
        tb = tb.tb_next
    return origin


def _resolve_owner(origin, current):
    # Attribute a missing dep to the connector whose module performed the failing import.
    # Importing one connector can transitively import another (e.g. virtual -> lories.typing
    # -> components -> components.openems -> connectors.openems -> websocket), and the dep
    # belongs to the imported connector, not the one the loop is currently on. None means
    # the failing import happened outside lories.connectors entirely.
    if origin is None:
        return current
    if not origin.startswith("lories.connectors."):
        return None
    key = origin[len("lories.connectors.") :]
    for name in CONNECTORS:
        if key == name or key.startswith(name + "."):
            return name
    return current


for _connector in CONNECTORS:
    while True:
        try:
            importlib.import_module(f".{_connector}", "lories.connectors")
            break

        except ImportError as e:
            _missing_dep = e.name or str(e).split("'")[1]
            _origin = _import_origin(e)
            _owner = _resolve_owner(_origin, _connector)

            if _is_mocked(_missing_dep):
                # Mocking this dependency didn't (or won't) help — give up on the module.
                if _owner is not None:
                    _logger.debug("Connector '%s' unavailable (missing: %s)", _owner, _missing_dep)
                    _unavailable[_owner] = _missing_dep
                else:
                    _logger.debug("Module '%s' unavailable (missing: %s)", _origin, _missing_dep)
                    _unavailable[str(_origin)] = _missing_dep
                break

            _logger.debug("Retrying '%s' with mock for missing dep '%s'", _connector, _missing_dep)
            _inject_mock(_missing_dep)
            if _owner is not None:
                _deps_by_owner.setdefault(_owner, []).append(_missing_dep)
            else:
                _logger.debug("Module '%s' unavailable (missing: %s)", _origin, _missing_dep)
                _unavailable[str(_origin)] = _missing_dep
            sys.modules.pop(f"lories.connectors.{_connector}", None)

for _connector, _deps in _deps_by_owner.items():
    _missing_dep = ", ".join(_deps)
    _logger.debug("Connector '%s' unavailable (missing: %s)", _connector, _missing_dep)
    _mocked[_connector] = _missing_dep

for _connector, _dep in _mocked.items():
    _mod = sys.modules.get(f"lories.connectors.{_connector}")
    if _mod is None:
        continue
    _mod_name = f"lories.connectors.{_connector}"
    for _obj in vars(_mod).values():
        # Match classes defined in the connector module itself or, for package
        # connectors (e.g. serial.i2c), anywhere inside the package.
        if isinstance(_obj, type) and (_obj.__module__ == _mod_name or _obj.__module__.startswith(_mod_name + ".")):
            _obj.__available__ = False
            _obj.__import_error__ = f"'{_dep}' is not installed."

del importlib


def unavailable() -> dict[str, str]:
    """Optional connectors whose dependencies are not installed, mapped to the missing distributions.

    Recorded here at import time and logged once by the application after logging is
    configured. Logging from this module would print a bare line per connector in every
    process that imports lories, including the spawned camera-stream workers.
    """
    return {**_unavailable, **_mocked}
