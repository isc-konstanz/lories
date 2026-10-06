# -*- coding: utf-8 -*-
"""
tests.test_connectors_sql_json
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

import json

import pytest
from sqlalchemy import Column as SqlColumn
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy import Table as SqlTable
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.schema import CreateTable
from sqlalchemy.types import INTEGER

import numpy as np
import pandas as pd
from lories.connectors.sql.columns.column import JsonType, parse_type, to_type_engine
from lories.connectors.sql.schema import Schema
from lories.core.configs.configurations import Configurations
from lories.core.resource import Resource
from lories.core.resources import Resources

GHI_ID = "sim.field.ghi"
SEGMENTS_ID = "sim.field.seg_ghi"


def _build_resources() -> Resources:
    common = dict(group="field", table="results")
    return Resources(
        [
            Resource(id=GHI_ID, key="ghi", type=float, **common),
            Resource(id=SEGMENTS_ID, key="seg_ghi", type=list, **common),
        ]
    )


def _connect(engine, tmp_path, resources):
    (tmp_path / "tables.conf").write_text("")
    configs = Configurations.load("tables.conf", data_dir=str(tmp_path), flat=True)
    schema = Schema(engine.dialect)
    schema.configure(configs)
    return schema.connect(engine, resources)


class _Rows:
    # sqlite reports rowcount -1 for SELECT, which Table.extract treats as an empty result.
    def __init__(self, result) -> None:
        self._rows = result.fetchall()
        self._keys = list(result.keys())
        self.rowcount = len(self._rows)

    def fetchall(self):
        return self._rows

    def keys(self):
        return self._keys


def test_parse_type_list_is_json():
    assert isinstance(parse_type(list, 255), JsonType)
    assert isinstance(parse_type("json"), JsonType)
    assert isinstance(to_type_engine("JSON"), JsonType)


def test_schema_connect_creates_list_column(tmp_path):
    engine = create_engine("sqlite://")

    tables = _connect(engine, tmp_path, _build_resources())

    assert "seg_ghi" in tables["results"].columns


def test_roundtrip_list_values_sqlite(tmp_path):
    engine = create_engine("sqlite://")
    resources = _build_resources()
    table = _connect(engine, tmp_path, resources)["results"]

    index = pd.to_datetime([f"2026-05-19 12:{m:02d}" for m in (0, 15, 30)]).tz_localize("UTC")
    frame = pd.DataFrame(
        {
            GHI_ID: [1.0, 2.0, 3.0],
            SEGMENTS_ID: pd.Series([[1.0, 2.5], [3.0, np.nan], np.nan], index=index, dtype=object),
        },
        index=index,
    )
    with engine.begin() as connection:
        connection.execute(table.write(resources, frame))

    with engine.connect() as connection:
        data = table.extract(resources, _Rows(connection.execute(table.read(resources, None, None))))
        null_count = connection.execute(text("SELECT COUNT(*) FROM results WHERE seg_ghi IS NULL")).scalar()
        null_rows = connection.execute(text("SELECT ghi FROM results WHERE seg_ghi IS NULL")).scalars().all()

    segments = data[SEGMENTS_ID].tolist()
    assert segments[0] == [1.0, 2.5]
    assert segments[1] == [3.0, None]
    assert pd.isna(segments[2])
    assert data[GHI_ID].tolist() == [1.0, 2.0, 3.0]
    assert null_count == 1
    assert null_rows == [3.0]


@pytest.mark.parametrize("dialect", [mysql.dialect(), postgresql.dialect()], ids=["mysql", "postgresql"])
def test_bind_processor_emits_valid_json(dialect):
    processor = JsonType().bind_processor(dialect)

    serialized = processor([1.0, float("nan"), np.float32(2.0), np.int64(3)])

    assert "NaN" not in serialized
    assert json.loads(serialized) == [1.0, None, 2.0, 3]


@pytest.mark.parametrize("dialect", [mysql.dialect(), postgresql.dialect()], ids=["mysql", "postgresql"])
def test_create_table_renders_json(dialect):
    table = SqlTable("results", MetaData(), SqlColumn("id", INTEGER, primary_key=True), SqlColumn("seg", JsonType()))

    assert "seg JSON" in str(CreateTable(table).compile(dialect=dialect))
