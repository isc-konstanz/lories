# -*- coding: utf-8 -*-
"""Tests for surrogate-group discovery on ``Table`` (``read_groups`` / ``extract_groups``).

Exercised at the ``Table`` level against an in-memory sqlite engine: ``SqlDatabase`` itself
has no sqlite dialect, so the connector-level ``read_groups`` (a thin delegation to these two
methods) is covered end-to-end by the ``remote_mirror`` shim test instead.
"""

from __future__ import annotations

from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.types import FLOAT, INTEGER, TIMESTAMP

import pytz
from lories.connectors.sql.columns import Column, DatetimeColumn, SurrogateKeyColumn
from lories.connectors.sql.table import Table


def _mirror_table() -> Table:
    metadata = MetaData()
    logged = DatetimeColumn("logged", TIMESTAMP, timezone=pytz.UTC, nullable=False, primary_key=True)
    creation = SurrogateKeyColumn("timestamp_creation", INTEGER, "creation")
    value = Column("value", FLOAT, nullable=True)
    return Table("mirror", metadata, logged, creation, value)


def _bind(table: Table):
    engine = create_engine("sqlite://")
    table.metadata.dialect = engine.dialect
    table.metadata.create_all(engine)
    return engine


def test_read_groups_returns_distinct_surrogate_attribute_dicts():
    table = _mirror_table()
    engine = _bind(table)
    timestamps = ["2027-01-01 00:00:00", "2027-01-01 01:00:00", "2027-01-01 02:00:00"]
    with engine.begin() as conn:
        for creation in (1001, 1002, 1003):
            for ts in timestamps:
                conn.execute(
                    text("INSERT INTO mirror (logged, timestamp_creation, value) VALUES (:l, :c, :v)"),
                    {"l": ts, "c": creation, "v": 1.0},
                )

    select = table.read_groups()
    assert select is not None
    with engine.connect() as conn:
        groups = table.extract_groups(conn.execute(select))

    # One dict per distinct creation group, keyed by the surrogate *attribute* ("creation"),
    # not the column name ("timestamp_creation").
    assert all(set(group.keys()) == {"creation"} for group in groups)
    assert sorted(group["creation"] for group in groups) == [1001, 1002, 1003]


def test_read_groups_none_without_surrogate_keys():
    metadata = MetaData()
    logged = DatetimeColumn("logged", TIMESTAMP, timezone=pytz.UTC, nullable=False, primary_key=True)
    value = Column("value", FLOAT, nullable=True)
    table = Table("plain", metadata, logged, value)

    assert table.read_groups() is None
