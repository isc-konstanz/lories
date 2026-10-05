# -*- coding: utf-8 -*-
"""
tests.test_connectors_tables_json
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

import numpy as np
import pandas as pd
from lories.application import Settings
from lories.application.main import Application
from lories.core.resource import Resource
from lories.core.resources import Resources

pytest.importorskip("tables")

SCALAR_ID = "sim.field.ghi"
LIST_ID = "sim.field.seg_ghi"
SOIL_ID = "sim.field.soil.top_in"
BLOB_ID = "sim.field.state"
PNG_ID = "sim.blob.png"


def _connect(tmp_path):
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    (tmp_path / "data").mkdir()
    (conf_dir / "settings.conf").write_text(
        'name = "tables_json"\n'
        "\n"
        "[interface]\n"
        "enabled = false\n"
        "\n"
        "[connectors.h5]\n"
        'type = "tables"\n'
        f'file = "{(tmp_path / "data" / "results.h5").as_posix()}"\n'
    )
    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        settings = Settings("tables_json")
        app = Application(settings)
        app.configure(settings)
    finally:
        os.chdir(cwd)

    resources = Resources(
        [
            Resource(id=SCALAR_ID, key="ghi", type=float, group="field"),
            Resource(id=LIST_ID, key="seg_ghi", type=list, group="field"),
            Resource(id=SOIL_ID, key="top_in", type=float, group="field_soil"),
            Resource(id=BLOB_ID, key="state", type=bytes, group="field"),
            Resource(id=PNG_ID, key="png", type=bytes, group="blob"),
        ]
    )
    database = app.connectors.get("h5")
    database._Connector__resources = resources
    database.connect(resources)
    return database, resources


def _frame(times, scalars, lists) -> pd.DataFrame:
    index = pd.DatetimeIndex(times, tz="UTC", name="timestamp")
    list_column = pd.Series(lists, index=index, dtype=object)
    return pd.DataFrame({SCALAR_ID: scalars, LIST_ID: list_column}, index=index)


T0 = pd.Timestamp("2026-01-01 00:00", tz="UTC")


@pytest.fixture
def database(tmp_path):
    database, resources = _connect(tmp_path)
    yield database, resources
    database.disconnect()


def test_round_trip_list_column(database):
    database, resources = database
    times = [T0 + pd.Timedelta(hours=h) for h in range(3)]
    database.write(_frame(times, [1.0, 2.0, 3.0], [[1.0, 2.5], [3.0, np.nan], None]))

    data = database.read(resources, T0, times[-1])

    assert data[LIST_ID].iloc[0] == [1.0, 2.5]
    assert data[LIST_ID].iloc[1] == [3.0, None]
    assert pd.isna(data[LIST_ID].iloc[2])
    assert list(data[SCALAR_ID]) == [1.0, 2.0, 3.0]


def test_append_longer_floats_than_first_write(database):
    database, resources = database
    first = [T0, T0 + pd.Timedelta(hours=1)]
    second = [T0 + pd.Timedelta(hours=2), T0 + pd.Timedelta(hours=3)]
    long_list = [123.45678901234567, -0.00012345678901234567]
    database.write(_frame(first, [1.0, 2.0], [[1.0, 2.5], [1.0, 2.5]]))
    database.write(_frame(second, [3.0, 4.0], [long_list, long_list]))

    data = database.read(resources, T0, second[-1])

    assert len(data) == 4
    assert data[LIST_ID].iloc[3] == long_list


def test_read_last_returns_list(database):
    database, resources = database
    times = [T0, T0 + pd.Timedelta(hours=1)]
    database.write(_frame(times, [1.0, 2.0], [[1.0, 2.5], [7.0, 8.0]]))

    data = database.read_last(resources)

    assert data[LIST_ID].iloc[0] == [7.0, 8.0]


def test_numpy_content_round_trips(database):
    database, resources = database
    database.write(_frame([T0], [1.0], [[np.float32(2.0), np.int64(3)]]))

    data = database.read(resources, T0, T0)

    assert data[LIST_ID].iloc[0] == [2.0, 3]


def _soil_frame(times, values) -> pd.DataFrame:
    return pd.DataFrame({SOIL_ID: values}, index=pd.DatetimeIndex(times, tz="UTC", name="timestamp"))


def test_read_skips_a_parent_path_that_holds_no_table(database):
    database, resources = database
    database.write(_soil_frame([T0], [1.0]))

    data = database.read(resources, T0, T0)

    assert list(data.columns) == [SOIL_ID]
    assert data[SOIL_ID].tolist() == [1.0]


def test_list_group_written_after_its_nested_child_keeps_the_width(database):
    database, resources = database
    long_list = [123.45678901234567, -0.00012345678901234567]
    database.write(_soil_frame([T0], [1.0]))
    database.write(_frame([T0 + pd.Timedelta(hours=1)], [1.0], [[1.0, 2.5]]))
    database.write(_frame([T0 + pd.Timedelta(hours=2)], [2.0], [long_list]))

    data = database.read(resources, T0, T0 + pd.Timedelta(hours=2))

    assert data[LIST_ID].dropna().tolist() == [[1.0, 2.5], long_list]
    assert data[SOIL_ID].dropna().tolist() == [1.0]


def _bytes_frame(times, scalars, lists, blobs) -> pd.DataFrame:
    frame = _frame(times, scalars, lists)
    frame[BLOB_ID] = pd.Series(blobs, index=frame.index, dtype=object)
    return frame


def test_bytes_column_is_skipped_on_write(database):
    database, resources = database
    times = [T0, T0 + pd.Timedelta(hours=1)]
    database.write(_bytes_frame(times, [1.0, 2.0], [[1.0, 2.5], [3.0, 4.0]], [b"ab", b"c"]))

    data = database.read(resources, T0, times[-1])

    assert list(data[SCALAR_ID]) == [1.0, 2.0]
    assert data[LIST_ID].iloc[1] == [3.0, 4.0]
    assert BLOB_ID not in data.columns


def test_group_with_only_bytes_writes_nothing(database):
    database, resources = database
    frame = pd.DataFrame({PNG_ID: [b"PNG"]}, index=pd.DatetimeIndex([T0], name="timestamp"))

    database.write(frame)

    assert database.read(resources, T0, T0).empty


def test_bytes_warning_logged_once(database):
    database, _ = database
    frame = _bytes_frame([T0], [1.0], [[1.0]], [b"a"])
    later = _bytes_frame([T0 + pd.Timedelta(hours=1)], [2.0], [[2.0]], [b"b"])

    with patch.object(database, "_logger") as logger:
        database.write(frame)
        database.write(later)

    assert logger.warning.call_count == 1
    assert BLOB_ID in logger.warning.call_args.args[0]
