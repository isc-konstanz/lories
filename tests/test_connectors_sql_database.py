# -*- coding: utf-8 -*-
"""
lories.tests.test_connectors_sql_database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unit tests for the per-operation pooled connection design of ``SqlDatabase``:
every data method checks a connection out of the engine pool for the duration
of one call and releases it on return AND on raise (so no implicit transaction
can outlive a call and pin a REPEATABLE-READ snapshot), writes and deletes run
as one transaction per call, and ``connect()`` still fails fast on credential
or connectivity errors.

The connector is instantiated via ``__new__`` against a mocked engine: the
``Registrator`` construction path needs a full application context, and
bypassing ``__init__`` also bypasses the ``ConnectorMeta`` method wrapping, so
the raw data methods are driven directly.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

import pandas as pd
from lories.connectors.errors import ConnectionError, DatabaseError
from lories.connectors.sql.database import SqlDatabase
from lories.core.resource import Resource
from lories.core.resources import Resources

ALPHA_ID = "unit_test.alpha_reading"
BETA_ID = "unit_test.beta_reading"


def _make_engine():
    engine = MagicMock()
    connection = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    engine.begin.return_value.__enter__.return_value = connection
    return engine, connection


def _make_resources(table: str = "alpha", id: str = ALPHA_ID, key: str = "reading") -> Resources:
    return Resources([Resource(id=id, key=key, name=key, type=float, column=key, table=table)])


def _make_database(engine, tables=None, resources=None) -> SqlDatabase:
    database = SqlDatabase.__new__(SqlDatabase)
    database.engine = engine
    database._logger = logging.getLogger("test_connectors_sql_database")
    database._SqlDatabase__tables = tables if tables is not None else {}
    if resources is not None:
        database._Connector__resources = resources
    return database


def _make_result_frame() -> pd.DataFrame:
    index = pd.DatetimeIndex([pd.Timestamp("2026-07-22 10:00", tz="UTC")], name="timestamp")
    return pd.DataFrame({ALPHA_ID: [1.0]}, index=index)


# ---------------------------------------------------------------- read paths release per operation


def test_read_checks_out_and_releases_pooled_connection():
    engine, connection = _make_engine()
    table = MagicMock()
    table.extract.return_value = _make_result_frame()
    connection.execute.return_value.rowcount = 1
    database = _make_database(engine, tables={"alpha": table})

    data = database.read(
        _make_resources(),
        pd.Timestamp("2026-07-22 09:00", tz="UTC"),
        pd.Timestamp("2026-07-22 11:00", tz="UTC"),
    )

    assert data[ALPHA_ID].tolist() == [1.0]
    engine.connect.assert_called_once()
    assert engine.connect.return_value.__exit__.called
    engine.begin.assert_not_called()


@pytest.mark.parametrize("method", ["read_first", "read_last"])
def test_read_first_last_check_out_and_release_pooled_connection(method):
    engine, connection = _make_engine()
    table = MagicMock()
    table.extract.return_value = _make_result_frame()
    connection.execute.return_value.rowcount = 1
    database = _make_database(engine, tables={"alpha": table})

    data = getattr(database, method)(_make_resources())

    assert data[ALPHA_ID].tolist() == [1.0]
    engine.connect.assert_called_once()
    assert engine.connect.return_value.__exit__.called


def test_exists_checks_out_and_releases_pooled_connection():
    engine, connection = _make_engine()
    table = MagicMock()
    connection.execute.return_value.rowcount = 1
    connection.execute.return_value.scalar.return_value = 2
    database = _make_database(engine, tables={"alpha": table})

    assert database.exists(_make_resources()) is True
    engine.connect.assert_called_once()
    assert engine.connect.return_value.__exit__.called


def test_hash_checks_out_and_releases_pooled_connection():
    engine, connection = _make_engine()
    table = MagicMock()
    connection.execute.return_value.rowcount = 1
    connection.execute.return_value.fetchall.return_value = [("abc123",)]
    database = _make_database(engine, tables={"alpha": table})

    assert database.hash(_make_resources()) == "abc123"
    engine.connect.assert_called_once()
    assert engine.connect.return_value.__exit__.called


def test_read_releases_connection_on_sqlalchemy_error():
    engine, connection = _make_engine()
    connection.execute.side_effect = SQLAlchemyError("server has gone away")
    database = _make_database(engine, tables={"alpha": MagicMock()})

    with pytest.raises(ConnectionError):
        database.read(_make_resources())
    assert engine.connect.return_value.__exit__.called


def test_read_raises_database_error_on_syntax_error():
    engine, connection = _make_engine()
    connection.execute.side_effect = SQLAlchemyError("You have an error in your SQL syntax")
    database = _make_database(engine, tables={"alpha": MagicMock()})

    with pytest.raises(DatabaseError):
        database.read(_make_resources())
    assert engine.connect.return_value.__exit__.called


def test_read_releases_connection_on_non_sql_error_in_extract():
    engine, connection = _make_engine()
    table = MagicMock()
    table.extract.side_effect = ValueError("malformed row")
    connection.execute.return_value.rowcount = 1
    database = _make_database(engine, tables={"alpha": table})

    with pytest.raises(ValueError):
        database.read(_make_resources())
    assert engine.connect.return_value.__exit__.called


# ---------------------------------------------------------------- write/delete: one transaction per call


def test_write_runs_all_tables_in_one_transaction():
    engine, connection = _make_engine()
    alpha = Resource(id=ALPHA_ID, key="reading", name="alpha", type=float, column="reading", table="alpha")
    beta = Resource(id=BETA_ID, key="reading", name="beta", type=float, column="reading", table="beta")
    resources = Resources([alpha, beta])
    database = _make_database(
        engine,
        tables={"alpha": MagicMock(), "beta": MagicMock()},
        resources=resources,
    )

    index = pd.DatetimeIndex([pd.Timestamp("2026-07-22 10:00", tz="UTC")], name="timestamp")
    database.write(pd.DataFrame({ALPHA_ID: [1.0], BETA_ID: [2.0]}, index=index))

    engine.begin.assert_called_once()
    assert connection.execute.call_count == 2
    assert engine.begin.return_value.__exit__.called
    engine.connect.assert_not_called()


def test_write_releases_transaction_on_error():
    engine, connection = _make_engine()
    connection.execute.side_effect = SQLAlchemyError("deadlock found")
    database = _make_database(engine, tables={"alpha": MagicMock()}, resources=_make_resources())

    index = pd.DatetimeIndex([pd.Timestamp("2026-07-22 10:00", tz="UTC")], name="timestamp")
    with pytest.raises(ConnectionError):
        database.write(pd.DataFrame({ALPHA_ID: [1.0]}, index=index))
    assert engine.begin.return_value.__exit__.called


def test_delete_runs_in_one_transaction_and_releases():
    engine, connection = _make_engine()
    database = _make_database(engine, tables={"alpha": MagicMock()})

    database.delete(_make_resources())

    engine.begin.assert_called_once()
    assert engine.begin.return_value.__exit__.called
    engine.connect.assert_not_called()


# ---------------------------------------------------------------- connect: fail fast, probe returns to pool


def _make_connectable_database(engine):
    database = _make_database(engine)
    database.dialect = SimpleNamespace(name="mysql")
    database.host = "localhost"
    database.port = 3306
    database.user = "user"
    database.database = "database"
    database._schema = MagicMock()
    database._schema.connect.return_value = {}
    return database


def test_connect_fails_fast_on_credential_error():
    engine, _ = _make_engine()
    engine.connect.side_effect = SQLAlchemyError("Access denied for user 'user'@'localhost'")
    database = _make_connectable_database(engine)

    with pytest.raises(ConnectionError):
        database.connect(_make_resources())


def test_connect_probes_and_returns_pooled_connection():
    engine, connection = _make_engine()
    connection.execute.return_value.scalar.return_value = "+00:00"
    database = _make_connectable_database(engine)

    database.connect(_make_resources())

    engine.connect.assert_called_once()
    assert engine.connect.return_value.__exit__.called
    database._schema.connect.assert_called_once()


def test_connect_raises_on_non_utc_session_timezone():
    engine, connection = _make_engine()
    connection.execute.return_value.scalar.return_value = "+02:00"
    database = _make_connectable_database(engine)

    with pytest.raises(DatabaseError):
        database.connect(_make_resources())
    assert engine.connect.return_value.__exit__.called


# ---------------------------------------------------------------- disconnect disposes the pool


def test_disconnect_disposes_engine():
    engine, _ = _make_engine()
    database = _make_database(engine)

    database.disconnect()

    engine.dispose.assert_called_once()


def test_disconnect_is_noop_without_engine():
    database = _make_database(engine=None)
    database.disconnect()


# ---------------------------------------------------------------- timezone pool event + is_connected


def test_pin_connection_timezone_executes_and_closes_cursor():
    dbapi_connection = MagicMock()
    cursor = dbapi_connection.cursor.return_value

    SqlDatabase._pin_connection_timezone(dbapi_connection, "SET time_zone = '+00:00'")

    cursor.execute.assert_called_once_with("SET time_zone = '+00:00'")
    cursor.close.assert_called_once()


def test_pin_connection_timezone_closes_cursor_on_error():
    dbapi_connection = MagicMock()
    cursor = dbapi_connection.cursor.return_value
    cursor.execute.side_effect = RuntimeError("connection reset")

    with pytest.raises(RuntimeError):
        SqlDatabase._pin_connection_timezone(dbapi_connection, "SET time_zone = '+00:00'")
    cursor.close.assert_called_once()


@pytest.mark.parametrize(
    "dialect, query",
    [
        ("mysql", "SET time_zone = '+00:00'"),
        ("mariadb", "SET time_zone = '+00:00'"),
        ("postgresql", "SET TIME ZONE '+00:00'"),
    ],
)
def test_utc_timezone_query_per_dialect(dialect, query):
    assert SqlDatabase._utc_timezone_query(dialect) == query


def test_utc_timezone_query_rejects_unknown_dialect():
    with pytest.raises(NotImplementedError):
        SqlDatabase._utc_timezone_query("sqlite")


def test_is_connected_pings_pool():
    engine, connection = _make_engine()
    database = _make_database(engine)

    assert database.is_connected() is True
    assert engine.connect.return_value.__exit__.called


def test_is_connected_false_without_engine():
    database = _make_database(engine=None)
    assert database.is_connected() is False


def test_is_connected_false_on_ping_error():
    engine, connection = _make_engine()
    connection.execute.side_effect = SQLAlchemyError("server has gone away")
    database = _make_database(engine)

    assert database.is_connected() is False
