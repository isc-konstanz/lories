# -*- coding: utf-8 -*-
"""
lories.tests.test_connectors_sql_timescale
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unit tests for the TimescaleDB connector: dialect pinning and config defaults
at configure time, extension creation gating, and the hypertable conversion
DDL issued for every connected table. The connector is instantiated via
``__new__`` against mocked connections, mirroring
``test_connectors_sql_database``; the actual ``create_hypertable`` behaviour
against a real TimescaleDB instance is an integration concern.
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


class _FakeConfigs:
    """Dict-backed stand-in for ``Configurations`` covering set/get/get_bool."""

    def __init__(self, configs=None) -> None:
        self._configs = dict(configs) if configs is not None else {}

    def set(self, key, value, replace=True) -> None:
        if key not in self._configs or replace:
            self._configs[key] = value

    def get(self, key, default=None):
        return self._configs.get(key, default)

    def get_bool(self, key, default=None) -> bool:
        return bool(self._configs.get(key, default))

    def __getitem__(self, key):
        return self._configs[key]

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


def _make_table(key="alpha", primary_columns=None):
    if primary_columns is None:
        primary_columns = [DatetimeColumn("timestamp", TIMESTAMP, timezone=tz.UTC, nullable=False, primary_key=True)]
    table = MagicMock()
    table.key = key
    table.primary_key.columns = primary_columns
    return table


# ---------------------------------------------------------------------------------- configure


def test_configure_pins_postgresql_dialect_and_defaults(monkeypatch):
    monkeypatch.setattr(SqlDatabase, "configure", lambda self, configs: None)
    database = _make_database()
    configs = _FakeConfigs()

    database.configure(configs)

    assert configs["dialect"] == "postgresql"
    assert database.chunk_interval == "7 days"
    assert database.create_extension is True
    assert database.migrate_data is False


def test_configure_accepts_explicit_settings(monkeypatch):
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

    database.configure(configs)

    assert database.chunk_interval == "1 day"
    assert database.create_extension is False
    assert database.migrate_data is True


def test_configure_rejects_other_dialects(monkeypatch):
    monkeypatch.setattr(SqlDatabase, "configure", lambda self, configs: None)
    database = _make_database()

    with pytest.raises(ConfigurationError):
        database.configure(_FakeConfigs({"dialect": "mysql"}))


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
