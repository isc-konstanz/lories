# -*- coding: utf-8 -*-
"""Regression: Table.write upsert writes the source row (not the stale target) on a duplicate key."""

from __future__ import annotations

import pytest
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.types import BOOLEAN, FLOAT, INTEGER

import pandas as pd
from lories.connectors.sql.columns import Column, SurrogateKeyColumn
from lories.connectors.sql.index import DatetimeIndexType
from lories.connectors.sql.table import Table
from lories.core.resource import Resource
from lories.core.resources import Resources

STATE_ID = "kob.agri_pv.apples.irrigation.state"
FLOW_ID = "kob.agri_pv.apples.irrigation.flow"


def _build_table(dialect) -> Table:
    metadata = MetaData()
    # lories reads the dialect off the schema (its MetaData); Table.write branches on its name.
    metadata.dialect = dialect
    index_columns = list(DatetimeIndexType.TIMESTAMP.columns("logged"))
    field_id = SurrogateKeyColumn("field_id", INTEGER, "field_id")
    flow = Column("flow", FLOAT, nullable=True)
    state = Column("irrigation_state", BOOLEAN, nullable=True)
    return Table("agri_field", metadata, *index_columns, field_id, flow, state)


def _build_resources() -> Resources:
    flow = Resource(id=FLOW_ID, key="flow", name="Irrigation Flow", type=float, column="flow", field_id=2)
    state = Resource(
        id=STATE_ID, key="state", name="Irrigation State", type=bool, column="irrigation_state", field_id=2
    )
    return Resources([flow, state])


def _build_data(state_values) -> pd.DataFrame:
    index = pd.to_datetime([f"2026-05-19 12:{m:02d}" for m in range(0, 15 * len(state_values), 15)]).tz_localize("UTC")
    flow_values = [1.0 + i for i in range(len(state_values))]
    return pd.DataFrame({FLOW_ID: flow_values, STATE_ID: state_values}, index=index)


# ---------------------------------------------------------------- upsert references the proposed row


# Production is MariaDB, but lories routes "mariadb" and "mysql" through the same Table.write
# branch and builds the statement with mysql.insert(...); the mysql dialect renders the same
# VALUES(col) a MariaDB connection would, so it pins that branch.
@pytest.mark.parametrize(
    "dialect, proposed_ref",
    [
        (mysql.dialect(), "irrigation_state = VALUES(irrigation_state)"),
        (postgresql.dialect(), "irrigation_state = excluded.irrigation_state"),
    ],
    ids=["mysql", "postgresql"],
)
def test_write_upsert_references_proposed_row(dialect, proposed_ref):
    table = _build_table(dialect)
    statement = table.write(_build_resources(), _build_data([False, False]))
    compiled = str(statement.compile(dialect=dialect, compile_kwargs={"literal_binds": False}))

    assert proposed_ref in compiled
    assert "agri_field.irrigation_state" not in compiled, "upsert must reference the proposed row, not the stored one"


# ---------------------------------------------------------------- value survives validate + insert


def test_write_roundtrip_preserves_boolean_sqlite():
    # SQLite takes Table.write's plain-insert branch, so this guards the _validate/insert value
    # path (the upsert SET clause is covered above): False must land as 0, not NULL.
    engine = create_engine("sqlite://")
    table = _build_table(engine.dialect)
    table.metadata.create_all(engine)

    statement = table.write(_build_resources(), _build_data([False, True, None]))
    with engine.begin() as connection:
        connection.execute(statement)
    with engine.connect() as connection:
        stored = connection.execute(text("SELECT irrigation_state FROM agri_field ORDER BY logged")).scalars().all()

    assert stored == [0, 1, None]
