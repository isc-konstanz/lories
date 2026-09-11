# -*- coding: utf-8 -*-
"""
Regression: Table.write upsert writes the source row (not the stale target) on a duplicate key;
Table.read_latest returns one row per surrogate group, not one row for the whole table.
"""

from __future__ import annotations

import pytest
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.types import BOOLEAN, FLOAT, INTEGER

import numpy as np
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


# ---------------------------------------------------------------- read_latest: one row per surrogate group


SOIL_A_ID = "kob.agri_pv.soil.3.water_tension"
SOIL_B_ID = "kob.agri_pv.soil.4.water_tension"


def _build_soil_table(dialect) -> Table:
    metadata = MetaData()
    metadata.dialect = dialect
    index_columns = list(DatetimeIndexType.TIMESTAMP.columns("timestamp"))
    soil_id = SurrogateKeyColumn("soil_id", INTEGER, "soil_id")
    water_tension = Column("water_tension", FLOAT, nullable=True)
    return Table("agri_soil", metadata, *index_columns, soil_id, water_tension)


def _build_soil_resources() -> Resources:
    soil_a = Resource(
        id=SOIL_A_ID, key="water_tension", name="Soil Water Tension", type=float, column="water_tension", soil_id=3
    )
    soil_b = Resource(
        id=SOIL_B_ID, key="water_tension", name="Soil Water Tension", type=float, column="water_tension", soil_id=4
    )
    return Resources([soil_a, soil_b])


def _build_soil_data() -> pd.DataFrame:
    # soil_id=3's latest row (12:15) sorts *before* soil_id=4's latest row (12:30), and
    # soil_id=3's earliest row (12:00) sorts *before* soil_id=4's earliest row (12:20): a
    # table-wide "ORDER BY timestamp {DESC,ASC} LIMIT 1" only ever returns one group's row.
    index = pd.to_datetime(
        ["2026-05-19 12:00", "2026-05-19 12:15", "2026-05-19 12:20", "2026-05-19 12:30"]
    ).tz_localize("UTC")
    return pd.DataFrame(
        {
            SOIL_A_ID: [10.0, 11.0, np.nan, np.nan],
            SOIL_B_ID: [np.nan, np.nan, 20.0, 21.0],
        },
        index=index,
    )


def _write_soil_rows(engine) -> Table:
    table = _build_soil_table(engine.dialect)
    table.metadata.create_all(engine)
    statement = table.write(_build_soil_resources(), _build_soil_data())
    with engine.begin() as connection:
        connection.execute(statement)
    return table


def test_read_latest_desc_returns_one_row_per_surrogate_group():
    # Mirrors SqlDatabase.read_last, which calls Table.read_latest(order_by="desc").
    engine = create_engine("sqlite://")
    table = _write_soil_rows(engine)

    with engine.connect() as connection:
        rows = connection.execute(table.read_latest(_build_soil_resources(), order_by="desc")).fetchall()

    by_soil_id = {row.soil_id: row for row in rows}
    assert set(by_soil_id) == {3, 4}
    assert by_soil_id[3].water_tension == pytest.approx(11.0)
    assert by_soil_id[4].water_tension == pytest.approx(21.0)


def test_read_latest_asc_returns_one_row_per_surrogate_group():
    # Mirrors SqlDatabase.read_first, which calls Table.read_latest(order_by="asc").
    engine = create_engine("sqlite://")
    table = _write_soil_rows(engine)

    with engine.connect() as connection:
        rows = connection.execute(table.read_latest(_build_soil_resources(), order_by="asc")).fetchall()

    by_soil_id = {row.soil_id: row for row in rows}
    assert set(by_soil_id) == {3, 4}
    assert by_soil_id[3].water_tension == pytest.approx(10.0)
    assert by_soil_id[4].water_tension == pytest.approx(20.0)


def test_read_latest_single_group_table_unchanged():
    # Regression: with only one surrogate group, read_latest must fetch the exact same row
    # as the pre-fix "ORDER BY ... LIMIT 1" query it replaces.
    engine = create_engine("sqlite://")
    table = _build_table(engine.dialect)
    table.metadata.create_all(engine)
    resources = _build_resources()
    statement = table.write(resources, _build_data([False, True, False]))
    with engine.begin() as connection:
        connection.execute(statement)

    with engine.connect() as connection:
        latest_rows = connection.execute(table.read_latest(resources, order_by="desc")).fetchall()
        plain_rows = connection.execute(table.read(resources, order_by="desc").limit(1)).fetchall()

    assert len(latest_rows) == 1
    assert latest_rows == plain_rows
