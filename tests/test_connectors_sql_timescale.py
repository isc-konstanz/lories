# -*- coding: utf-8 -*-
"""
lories.tests.test_connectors_sql_timescale
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unit tests for the TimescaleDB connector: parameter resolution (dialect
pinning, defaults and explicit values), extension creation gating, and the
hypertable conversion DDL issued for every connected table. The connector is
instantiated via ``__new__`` against mocked connections, mirroring
``test_connectors_sql_database``; because that bypasses ``_at_configure``,
the parameter descriptors are resolved explicitly from
``__config_parameters__``. The actual ``create_hypertable`` behaviour against
a real TimescaleDB instance is an integration concern.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
from sqlalchemy.types import TIMESTAMP

import pytz as tz
from lories.connectors.errors import DatabaseError
from lories.connectors.sql.columns import DatetimeColumn
from lories.connectors.sql.database import SqlDatabase
from lories.connectors.sql.timescale import TimescaleDatabase
from lories.core.configs import ConfigurationError

PARAMETER_ATTRS = ("_dialect", "_chunk_interval", "_create_extension", "_migrate_data")


class _FakeConfigs:
    """Dict-backed stand-in for ``Configurations`` covering the accessors
    used by parameter resolution (``in``, ``get``, ``get_bool``)."""

    def __init__(self, configs=None) -> None:
        self._configs = dict(configs) if configs is not None else {}

    def get(self, key, default=None):
        return self._configs.get(key, default)

    def get_bool(self, key, default=None) -> bool:
        return bool(self._configs.get(key, default))

    def __contains__(self, key) -> bool:
        return key in self._configs


def _make_database(tables=None, chunk_interval="7 days", create_extension=True, migrate_data=False):
    database = TimescaleDatabase.__new__(TimescaleDatabase)
    database._logger = logging.getLogger("test_connectors_sql_timescale")
    database._SqlDatabase__tables = tables if tables is not None else {}
    database.chunk_interval = chunk_interval
    database.create_extension = create_extension
    database.migrate_data = migrate_data
    return database


def _resolve_parameters(database, configs) -> None:
    # Mirrors the slice of Configurator._at_configure covering this connector's
    # own parameters; the inherited connection parameters stay untouched.
    for attr in PARAMETER_ATTRS:
        setattr(database, attr, TimescaleDatabase.__config_parameters__[attr].resolve(configs))


def _make_table(key="alpha", primary_columns=None):
    if primary_columns is None:
        primary_columns = [DatetimeColumn("timestamp", TIMESTAMP, timezone=tz.UTC, nullable=False, primary_key=True)]
    table = MagicMock()
    table.key = key
    table.primary_key.columns = primary_columns
    return table


# ---------------------------------------------------------------------------------- parameters


def test_parameters_registered_on_class():
    for attr in PARAMETER_ATTRS:
        assert attr in TimescaleDatabase.__config_parameters__


def test_parameters_resolve_defaults_and_pin_dialect(monkeypatch):
    monkeypatch.setattr(SqlDatabase, "configure", lambda self, configs: None)
    database = _make_database()
    configs = _FakeConfigs()

    _resolve_parameters(database, configs)
    database.configure(configs)

    assert database._dialect == "postgresql"
    assert database.chunk_interval == "7 days"
    assert database.create_extension is True
    assert database.migrate_data is False


def test_parameters_resolve_explicit_settings(monkeypatch):
    monkeypatch.setattr(SqlDatabase, "configure", lambda self, configs: None)
    database = _make_database()
    configs = _FakeConfigs(
        {
            "dialect": "postgresql",
            "chunk_interval": "1 day",
            "create_extension": False,
            "migrate_data": True,
        }
    )

    _resolve_parameters(database, configs)
    database.configure(configs)

    assert database.chunk_interval == "1 day"
    assert database.create_extension is False
    assert database.migrate_data is True


def test_dialect_parameter_rejects_other_dialects():
    database = _make_database()

    with pytest.raises(ConfigurationError):
        _resolve_parameters(database, _FakeConfigs({"dialect": "mysql"}))


# ---------------------------------------------------------------------------------- hypertable DDL


def test_create_hypertables_runs_extension_then_one_conversion_per_table():
    tables = {"alpha": _make_table("alpha"), "beta": _make_table("beta")}
    database = _make_database(tables=tables)
    connection = MagicMock()

    database._create_hypertables(connection)

    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert statements[0] == "CREATE EXTENSION IF NOT EXISTS timescaledb"
    assert len(statements) == 3
    assert all("create_hypertable" in statement for statement in statements[1:])

    converted = [call.args[1]["table"] for call in connection.execute.call_args_list[1:]]
    assert converted == ["alpha", "beta"]


def test_create_hypertables_skips_extension_when_disabled():
    database = _make_database(tables={"alpha": _make_table("alpha")}, create_extension=False)
    connection = MagicMock()

    database._create_hypertables(connection)

    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert len(statements) == 1
    assert "create_hypertable" in statements[0]


def test_create_hypertable_passes_partition_column_and_options():
    database = _make_database(chunk_interval="1 day", migrate_data=True)
    table = _make_table("schema.alpha")
    connection = MagicMock()

    database._create_hypertable(connection, table)

    statement, params = connection.execute.call_args.args
    assert "if_not_exists => TRUE" in str(statement)
    assert params == {
        "table": "schema.alpha",
        "column": "timestamp",
        "interval": "1 day",
        "migrate": True,
    }


def test_create_hypertable_requires_datetime_primary_key():
    database = _make_database()
    surrogate = MagicMock()
    surrogate.name = "id"
    table = _make_table("alpha", primary_columns=[surrogate])

    with pytest.raises(DatabaseError):
        database._create_hypertable(MagicMock(), table)
