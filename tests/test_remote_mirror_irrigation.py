# -*- coding: utf-8 -*-
"""Faithful end-to-end mirror test shaped like the sparcs irrigation schedule table.

Models ``agri_field_forecast_irrigation``: PK = (``timestamp`` future index, ``timestamp_creation``
TIMESTAMP surrogate = the predictor run time), value column ``irrigation_state``. Two computation
runs append overlapping future rows under distinct ``timestamp_creation`` values; the mirror must
copy BOTH runs (the append-per-run history), discovering the run timestamps live. Uses a sqlite
``Database`` shim over the real lories ``Table`` (``SqlDatabase`` has no sqlite dialect). Marked
``slow`` (SQL I/O).
"""

from __future__ import annotations

import os
import tempfile

import pytest

pytestmark = pytest.mark.slow


def _register_irrigation_database():
    from sqlalchemy import create_engine
    from sqlalchemy.types import FLOAT, TIMESTAMP

    import pandas as pd
    import pytz
    from lories.connectors import Database, register_connector_type
    from lories.connectors.sql.columns import Column, DatetimeColumn, SurrogateKeyColumn
    from lories.connectors.sql.table import Table

    def _make_table() -> Table:
        from sqlalchemy import MetaData

        metadata = MetaData()
        timestamp = DatetimeColumn("timestamp", TIMESTAMP, timezone=pytz.UTC, nullable=False, primary_key=True)
        creation = SurrogateKeyColumn("timestamp_creation", TIMESTAMP, "creation")
        state = Column("irrigation_state", FLOAT, nullable=True)
        return Table("agri_field_forecast_irrigation", metadata, timestamp, creation, state)

    class _ResultShim:
        def __init__(self, rows, keys):
            self._rows, self._keys = rows, keys

        @property
        def rowcount(self):
            return len(self._rows)

        def fetchall(self):
            return self._rows

        def keys(self):
            return self._keys

    @register_connector_type("sqlite_irrigation", replace=True)
    class SqliteIrrigationDatabase(Database):
        engine = None
        table = None
        _connection = None
        _db_path = None

        def connect(self, resources) -> None:
            if self.engine is None:
                handle, self._db_path = tempfile.mkstemp(suffix=".db")
                os.close(handle)
                self.engine = create_engine(f"sqlite:///{self._db_path}")
                self.table = _make_table()
                self.table.metadata.dialect = self.engine.dialect
                self.table.metadata.create_all(self.engine)
            if self._connection is None:
                self._connection = self.engine.connect()

        def disconnect(self) -> None:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

        def is_connected(self) -> bool:
            return self._connection is not None

        def _exec(self, select, resources) -> pd.DataFrame:
            result = self._connection.execute(select)
            shim = _ResultShim(result.fetchall(), list(result.keys()))
            if shim.rowcount <= 0:
                return pd.DataFrame()
            data = self.table.extract(resources, shim)
            return data if not data.empty else pd.DataFrame()

        def read(self, resources, start=None, end=None) -> pd.DataFrame:
            if start is None and end is None:
                select = self.table.read(resources, order_by="desc").limit(1)
            else:
                select = self.table.read(resources, start, end)
            return self._exec(select, resources)

        def read_first(self, resources):
            return self._exec(self.table.read(resources, order_by="asc").limit(1), resources)

        def read_last(self, resources):
            return self._exec(self.table.read(resources, order_by="desc").limit(1), resources)

        def read_groups(self, resources):
            select = self.table.read_groups()
            if select is None:
                return []
            return self.table.extract_groups(self._connection.execute(select))

        def write(self, data: pd.DataFrame) -> None:
            columns = [r.id for r in self.resources if r.id in data.columns]
            subset = data.loc[:, columns].dropna(axis="index", how="all")
            if subset.empty:
                return
            self._connection.execute(self.table.write(self.resources, subset))
            self._connection.commit()

        def delete(self, resources, start=None, end=None) -> None:
            self._connection.execute(self.table.delete(resources, start, end))
            self._connection.commit()

    return SqliteIrrigationDatabase


def _build_application(tmp_dir: str):
    from lories.application import Settings
    from lories.application.main import Application

    conf_dir = os.path.join(tmp_dir, "conf")
    os.makedirs(conf_dir)
    os.makedirs(os.path.join(tmp_dir, "data"))
    settings_text = (
        'name = "irrigation_mirror"\n'
        'action = "run"\n\n'
        "[interface]\nenabled = false\n\n"
        '[connectors.server]\ntype = "sqlite_irrigation"\ntimezone = "UTC"\n\n'
        '[connectors.edge]\ntype = "sqlite_irrigation"\ntimezone = "UTC"\n\n'
        "[components.copy_schedule]\n"
        'type = "remote_mirror"\n'
        'source = "server"\n'
        'target = "edge"\n'
        'mode = "pull"\n'
        "full = true\n"
    )
    with open(os.path.join(conf_dir, "settings.conf"), "w") as file:
        file.write(settings_text)

    cwd = os.getcwd()
    os.chdir(tmp_dir)
    try:
        settings = Settings("irrigation_mirror")
        app = Application(settings)
        app.configure(settings)
    finally:
        os.chdir(cwd)
    return app


def test_mirror_copies_all_computation_runs():
    from sqlalchemy import text

    import pandas as pd
    from lories.components import RemoteMirror
    from lories.data.channels import Channels

    _register_irrigation_database()
    app = _build_application(tempfile.mkdtemp(prefix="irrigation_mirror_"))

    mirror = app.components["copy_schedule"]
    assert isinstance(mirror, RemoteMirror)

    # The mirrored schedule channel: table + column, no surrogate value (runs discovered live).
    mirror.data.add(
        "irrigation_state",
        table="agri_field_forecast_irrigation",
        column="irrigation_state",
        type="float",
    )
    declared = Channels(list(mirror.data.values()))

    server = app.connectors.get("server")
    # Two predictor runs (transaction time), each planning the same 4 future timestamps.
    runs = [pd.Timestamp("2027-06-01 06:00", tz="UTC"), pd.Timestamp("2027-06-01 18:00", tz="UTC")]
    horizon = pd.date_range("2027-06-02 00:00", periods=4, freq="6h", tz="UTC")

    seed_resources = Channels(
        [
            channel.duplicate(id=f"{channel.id}.{index}", creation=run)
            for channel in declared
            for index, run in enumerate(runs)
        ]
    )
    server.connect(seed_resources)
    seed_frame = pd.DataFrame(index=horizon)
    for channel in declared:
        for index, _run in enumerate(runs):
            seed_frame[f"{channel.id}.{index}"] = [1.0, 0.0, 1.0, 0.0]
    server.write(seed_frame)
    server.disconnect()

    mirror._mirror_once()

    edge = app.connectors.get("edge")
    with edge.engine.connect() as connection:
        rows = connection.execute(text("SELECT COUNT(*) FROM agri_field_forecast_irrigation")).scalar()
        run_count = connection.execute(
            text("SELECT COUNT(DISTINCT timestamp_creation) FROM agri_field_forecast_irrigation")
        ).scalar()

    # Both runs' full future horizon mirrored to the edge (append-per-run history preserved).
    assert rows == len(runs) * len(horizon)
    assert run_count == len(runs)
