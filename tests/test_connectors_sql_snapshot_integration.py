# -*- coding: utf-8 -*-
"""
lories.tests.test_connectors_sql_snapshot_integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration regression against a real MySQL/MariaDB server: a ranged
``SqlDatabase.read`` issued AFTER another session committed a newer row must
return that row.

On the shared long-lived connection design, SQLAlchemy 2.x opens an implicit
transaction on the first ``execute()`` and the read paths never commit, so
InnoDB REPEATABLE READ pins that transaction's snapshot -- the connector then
reads a frozen copy of the database for the rest of the process lifetime
(observed in production as a field simulation whose data frontier froze at
process start time until the next restart).

The server target defaults to the local throwaway docker container
(``mysql-db-1``, 127.0.0.1:13306, root/example) and is overridable via
``LORIES_TEST_SQL_HOST`` / ``LORIES_TEST_SQL_PORT`` / ``LORIES_TEST_SQL_USER``
/ ``LORIES_TEST_SQL_PASSWORD``. The test creates and drops its own scratch
database and skips cleanly when no server is reachable.
"""

from __future__ import annotations

import os

import pytest

import pandas as pd

pytestmark = pytest.mark.slow

_HOST = os.environ.get("LORIES_TEST_SQL_HOST", "127.0.0.1")
_PORT = int(os.environ.get("LORIES_TEST_SQL_PORT", "13306"))
_USER = os.environ.get("LORIES_TEST_SQL_USER", "root")
_PASSWORD = os.environ.get("LORIES_TEST_SQL_PASSWORD", "example")

_DATABASE = "lories_test_sql_snapshot"
_TABLE = "snapshot_probe"
_READING_ID = "snapshot_test.reading"

_SETTINGS_CONF = """
name = "lories_snapshot_test"
action = "run"

[interface]
enabled = false
"""

_SYSTEM_CONF = f"""
key = "snapshot_test"
name = "Snapshot Integration Test"

[connectors.sql]
type = "sql"
enabled = true
dialect = "mysql"
host = "{_HOST}"
port = {_PORT}
user = "{_USER}"
password = "{_PASSWORD}"
database = "{_DATABASE}"
"""


def _server_engine():
    """Engine against the server (no schema) for scratch-database DDL and the
    second, independent writer session."""
    from sqlalchemy import create_engine

    return create_engine(
        f"mysql+pymysql://{_USER}:{_PASSWORD}@{_HOST}:{_PORT}/",
        connect_args={"connect_timeout": 3},
    )


def _build_resources():
    from lories.core.resource import Resource
    from lories.core.resources import Resources

    reading = Resource(
        id=_READING_ID,
        key="reading",
        name="Snapshot Reading",
        type=float,
        column="reading",
        table=_TABLE,
    )
    return Resources([reading])


@pytest.fixture
def sql_connector(tmp_path, monkeypatch):
    pytest.importorskip("sqlalchemy")
    pytest.importorskip("pymysql")
    lories = pytest.importorskip("lories")
    from sqlalchemy import text

    server = _server_engine()
    try:
        with server.connect() as connection:
            connection.execute(text("SELECT 1"))
    except Exception as e:  # noqa: BLE001
        server.dispose()
        pytest.skip(f"No MySQL/MariaDB server reachable on {_HOST}:{_PORT}: {e}")

    with server.begin() as connection:
        connection.execute(text(f"DROP DATABASE IF EXISTS `{_DATABASE}`"))
        connection.execute(text(f"CREATE DATABASE `{_DATABASE}`"))

    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    (conf_dir / "settings.conf").write_text(_SETTINGS_CONF)
    (conf_dir / "system.conf").write_text(_SYSTEM_CONF)
    monkeypatch.chdir(tmp_path)

    app = lories.load("lories_snapshot_test")

    connector = None
    for candidate in app.connectors.values():
        if candidate.key == "sql":
            connector = candidate
            break
    assert connector is not None, "connectors.sql not found on the loaded headless project"

    resources = _build_resources()
    connector.connect(resources)

    yield connector, resources, server

    try:
        connector.disconnect()
    except Exception:  # noqa: BLE001
        pass
    try:
        with server.begin() as connection:
            connection.execute(text(f"DROP DATABASE IF EXISTS `{_DATABASE}`"))
    finally:
        server.dispose()


def test_ranged_read_sees_row_committed_by_second_session(sql_connector):
    from sqlalchemy import text

    connector, resources, server = sql_connector

    start = pd.Timestamp("2026-07-22 09:00", tz="UTC")
    end = pd.Timestamp("2026-07-22 11:00", tz="UTC")
    first_ts = pd.Timestamp("2026-07-22 10:00", tz="UTC")
    second_ts = pd.Timestamp("2026-07-22 10:05", tz="UTC")

    frame = pd.DataFrame({_READING_ID: [1.0]}, index=pd.DatetimeIndex([first_ts], name="timestamp"))
    connector.write(frame)

    before = connector.read(resources, start, end)
    assert first_ts in before.index
    assert second_ts not in before.index

    # A second, independent session commits a newer row -- another process
    # (logger, replication, manual insert) writing while this one keeps running.
    with server.begin() as connection:
        connection.execute(text("SET time_zone = '+00:00'"))
        connection.execute(
            text(f"INSERT INTO `{_DATABASE}`.`{_TABLE}` (`timestamp`, `reading`) VALUES (:timestamp, :reading)"),
            {"timestamp": "2026-07-22 10:05:00", "reading": 2.0},
        )

    after = connector.read(resources, start, end)
    assert second_ts in after.index, (
        "ranged read must return the row committed by the second session; a REPEATABLE-READ "
        "snapshot pinned by this connector's first read has frozen its view of the database"
    )
    assert after.loc[second_ts, _READING_ID] == 2.0
