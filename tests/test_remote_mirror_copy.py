# -*- coding: utf-8 -*-
"""End-to-end copy test for ``RemoteMirror._mirror_once`` via a sqlite-backed ``Database`` shim.

``SqlDatabase`` has no sqlite dialect, so this registers a ``Database`` subclass that delegates
to the real lories ``Table`` (genuine surrogate SQL incl. ``read_groups``/``extract_groups`` and
the upsert write path) over a temp-file sqlite engine. A file — not ``:memory:`` — is used so the
component's own connect/disconnect cycle does not wipe seeded data. Marked ``slow`` (SQL I/O).
"""

from __future__ import annotations

import os
import tempfile

import pytest

pytestmark = pytest.mark.slow


def _register_sqlite_mirror_database():
    from sqlalchemy import create_engine
    from sqlalchemy.types import FLOAT, INTEGER, TIMESTAMP

    import pandas as pd
    import pytz
    from lories.connectors import Database, register_connector_type
    from lories.connectors.sql.columns import Column, DatetimeColumn, SurrogateKeyColumn
    from lories.connectors.sql.table import Table

    def _make_table() -> Table:
        from sqlalchemy import MetaData

        metadata = MetaData()
        logged = DatetimeColumn("logged", TIMESTAMP, timezone=pytz.UTC, nullable=False, primary_key=True)
        creation = SurrogateKeyColumn("timestamp_creation", INTEGER, "creation")
        value = Column("value", FLOAT, nullable=True)
        return Table("mirror", metadata, logged, creation, value)

    class _ResultShim:
        # sqlite leaves Result.rowcount == -1 for SELECTs, but Table.extract gates on rowcount > 0.
        def __init__(self, rows, keys):
            self._rows, self._keys = rows, keys

        @property
        def rowcount(self):
            return len(self._rows)

        def fetchall(self):
            return self._rows

        def keys(self):
            return self._keys

    @register_connector_type("sqlite_mirror", replace=True)
    class SqliteMirrorDatabase(Database):
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

        def read_groups(self, resources, start=None, end=None):
            select = self.table.read_groups(start, end)
            if select is None:
                return None
            return self.table.extract_groups(self._connection.execute(select))

        def write(self, data: pd.DataFrame) -> None:
            columns = [r.id for r in self.resources if r.id in data.columns]
            subset = data.loc[:, columns].dropna(axis="index", how="all")
            if subset.empty:
                return
            # Real dialects (mysql/postgres) upsert on duplicate primary keys; sqlite's
            # equivalent is INSERT OR REPLACE, keeping repeated mirror runs re-writable.
            self._connection.execute(self.table.write(self.resources, subset).prefix_with("OR REPLACE"))
            self._connection.commit()

        def delete(self, resources, start=None, end=None) -> None:
            self._connection.execute(self.table.delete(resources, start, end))
            self._connection.commit()

    return SqliteMirrorDatabase


def _build_application(
    tmp_dir: str, mode: str = "pull", source: str = "remote", target: str = "local", extra: str = ""
):
    from lories.application import Settings
    from lories.application.main import Application

    conf_dir = os.path.join(tmp_dir, "conf")
    os.makedirs(conf_dir)
    os.makedirs(os.path.join(tmp_dir, "data"))
    settings_text = (
        'name = "mirror_copy"\n'
        'action = "run"\n\n'
        "[interface]\nenabled = false\n\n"
        '[connectors.remote]\ntype = "sqlite_mirror"\ntimezone = "UTC"\n\n'
        '[connectors.local]\ntype = "sqlite_mirror"\ntimezone = "UTC"\n\n'
        "[components.sched]\n"
        'type = "remote_mirror"\n'
        f'source = "{source}"\n' + (f'target = "{target}"\n' if target is not None else "") + f'mode = "{mode}"\n'
        "full = true\n" + extra
    )
    with open(os.path.join(conf_dir, "settings.conf"), "w") as file:
        file.write(settings_text)

    cwd = os.getcwd()
    os.chdir(tmp_dir)
    try:
        settings = Settings("mirror_copy")
        app = Application(settings)
        app.configure(settings)
    finally:
        os.chdir(cwd)
    return app


def test_mirror_once_copies_all_surrogate_groups():
    from sqlalchemy import text

    import pandas as pd
    from lories.components import RemoteMirror
    from lories.data.channels import Channels

    _register_sqlite_mirror_database()
    app = _build_application(tempfile.mkdtemp(prefix="mirror_copy_"))

    mirror = app.components["sched"]
    assert isinstance(mirror, RemoteMirror)

    # Inject the mirrored channel (table "mirror", column "value") — no surrogate value:
    # the creation groups are discovered live at copy time.
    mirror.data.add("value", table="mirror", column="value", type="float")
    declared = Channels(list(mirror.data.values()))

    source = app.connectors.get("remote")
    creations = [1001, 1002, 1003]
    future = pd.date_range("2027-01-01 00:00", periods=5, freq="1h", tz="UTC")

    # Seed the source with 3 creation groups over the same 5 future timestamps, going through
    # the real Table.write path (per-group duplicated resources) so datetimes are stored exactly
    # as Table.read expects.
    seed_resources = Channels(
        [channel.duplicate(id=f"{channel.id}.{c}", creation=c) for channel in declared for c in creations]
    )
    source.connect(seed_resources)
    seed_frame = pd.DataFrame(index=future)
    for channel in declared:
        for creation in creations:
            seed_frame[f"{channel.id}.{creation}"] = float(creation)
    source.write(seed_frame)
    source.disconnect()

    mirror._mirror_once()

    target = app.connectors.get("local")
    with target.engine.connect() as connection:
        rows = connection.execute(text("SELECT COUNT(*) FROM mirror")).scalar()
        groups = sorted(
            row[0] for row in connection.execute(text("SELECT DISTINCT timestamp_creation FROM mirror")).fetchall()
        )

    assert rows == len(creations) * len(future)
    assert groups == creations


def test_push_mode_copies_local_to_remote():
    from sqlalchemy import text

    import pandas as pd
    from lories.data.channels import Channels

    _register_sqlite_mirror_database()
    app = _build_application(tempfile.mkdtemp(prefix="mirror_push_"), mode="push")

    mirror = app.components["sched"]
    mirror.data.add("value", table="mirror", column="value", type="float")
    declared = Channels(list(mirror.data.values()))

    creations = [2001, 2002]
    future = pd.date_range("2027-01-01 00:00", periods=3, freq="1h", tz="UTC")

    # In "push" mode the LOCAL database (target="local") is the copy source; seed it.
    local = app.connectors.get("local")
    seed_resources = Channels(
        [
            channel.duplicate(id=f"{channel.id}.{index}", creation=c)
            for channel in declared
            for index, c in enumerate(creations)
        ]
    )
    local.connect(seed_resources)
    seed_frame = pd.DataFrame(index=future)
    for channel in declared:
        for index, creation in enumerate(creations):
            seed_frame[f"{channel.id}.{index}"] = float(creation)
    local.write(seed_frame)
    local.disconnect()

    mirror._mirror_once()

    remote = app.connectors.get("remote")
    with remote.engine.connect() as connection:
        rows = connection.execute(text("SELECT COUNT(*) FROM mirror")).scalar()
    assert rows == len(creations) * len(future)


def test_pull_without_target_defaults_to_logger_database():
    from sqlalchemy import text

    import pandas as pd
    from lories.data.channels import Channels

    _register_sqlite_mirror_database()
    app = _build_application(tempfile.mkdtemp(prefix="mirror_logger_"), target=None)

    mirror = app.components["sched"]
    mirror.data.add("value", table="mirror", column="value", type="float", logger={"connector": "local"})
    declared = Channels(list(mirror.data.values()))

    source = app.connectors.get("remote")
    creations = [3001, 3002]
    future = pd.date_range("2027-01-01 00:00", periods=3, freq="1h", tz="UTC")

    seed_resources = Channels(
        [channel.duplicate(id=f"{channel.id}.{c}", creation=c) for channel in declared for c in creations]
    )
    source.connect(seed_resources)
    seed_frame = pd.DataFrame(index=future)
    for channel in declared:
        for creation in creations:
            seed_frame[f"{channel.id}.{creation}"] = float(creation)
    source.write(seed_frame)
    source.disconnect()

    mirror._mirror_once()

    local = app.connectors.get("local")
    with local.engine.connect() as connection:
        rows = connection.execute(text("SELECT COUNT(*) FROM mirror")).scalar()
    assert rows == len(creations) * len(future)


def test_missing_target_without_logger_raises():
    from lories.components import ComponentError

    _register_sqlite_mirror_database()
    app = _build_application(tempfile.mkdtemp(prefix="mirror_nolog_"), target=None)

    mirror = app.components["sched"]
    mirror.data.add("value", table="mirror", column="value", type="float")

    with pytest.raises(ComponentError):
        mirror._mirror_once()


def test_source_equal_target_raises():
    from lories.components import ComponentError

    _register_sqlite_mirror_database()
    app = _build_application(tempfile.mkdtemp(prefix="mirror_same_"), source="remote", target="remote")

    mirror = app.components["sched"]
    mirror.data.add("value", table="mirror", column="value", type="float")

    with pytest.raises(ComponentError):
        mirror._mirror_once()


def _seed_past_and_future(app, creations=(3001, 3002)):
    """Seed the source with a past-only creation group and a group spanning past and future.

    Returns (channel, now, past_index, future_index): group ``creations[0]`` has only the past
    rows, group ``creations[1]`` the same past rows plus all future rows.
    """
    import pandas as pd
    from lories.data.channels import Channels

    mirror = app.components["sched"]
    mirror.data.add("value", table="mirror", column="value", type="float")
    channel = list(mirror.data.values())[0]

    now = pd.Timestamp.now(tz="UTC")
    past = pd.DatetimeIndex([now - pd.Timedelta(hours=3), now - pd.Timedelta(hours=2)])
    future = pd.DatetimeIndex([now + pd.Timedelta(hours=1), now + pd.Timedelta(hours=2)])

    source = app.connectors.get("remote")
    seed_resources = Channels([channel.duplicate(id=f"{channel.id}.{c}", creation=c) for c in creations])
    source.connect(seed_resources)
    frame_past = pd.DataFrame(index=past)
    frame_past[f"{channel.id}.{creations[0]}"] = float(creations[0])
    frame_past[f"{channel.id}.{creations[1]}"] = float(creations[1])
    source.write(frame_past)
    frame_future = pd.DataFrame(index=future)
    frame_future[f"{channel.id}.{creations[1]}"] = float(creations[1])
    source.write(frame_future)
    source.disconnect()
    return channel, now, past, future


def test_forecast_window_copies_only_future_rows_and_groups():
    from sqlalchemy import text

    _register_sqlite_mirror_database()
    app = _build_application(tempfile.mkdtemp(prefix="mirror_forecast_"), extra='window = "forecast"\n')
    _, _, past, future = _seed_past_and_future(app)

    app.components["sched"]._mirror_once()

    target = app.connectors.get("local")
    with target.engine.connect() as connection:
        rows = connection.execute(text("SELECT COUNT(*) FROM mirror")).scalar()
        groups = sorted(
            row[0] for row in connection.execute(text("SELECT DISTINCT timestamp_creation FROM mirror")).fetchall()
        )

    # Only the group with future rows is discovered, and only its future rows are copied.
    assert rows == len(future)
    assert groups == [3002]


def test_historical_window_copies_only_past_rows():
    from sqlalchemy import text

    _register_sqlite_mirror_database()
    app = _build_application(tempfile.mkdtemp(prefix="mirror_hist_"), extra='window = "historical"\n')
    _, _, past, future = _seed_past_and_future(app)

    app.components["sched"]._mirror_once()

    target = app.connectors.get("local")
    with target.engine.connect() as connection:
        rows = connection.execute(text("SELECT COUNT(*) FROM mirror")).scalar()
        groups = sorted(
            row[0] for row in connection.execute(text("SELECT DISTINCT timestamp_creation FROM mirror")).fetchall()
        )

    # Both groups have past rows; none of the future rows crosses over.
    assert rows == 2 * len(past)
    assert groups == [3001, 3002]


def test_forecast_window_propagates_source_deletions_by_default():
    from sqlalchemy import text

    import pandas as pd
    from lories.data.channels import Channels

    _register_sqlite_mirror_database()
    # 'force' stays unset: the forecast window must default to an exact (deletion-propagating) mirror.
    app = _build_application(tempfile.mkdtemp(prefix="mirror_cancel_"), extra='window = "forecast"\n')

    mirror = app.components["sched"]
    mirror.data.add("value", table="mirror", column="value", type="float")
    channel = list(mirror.data.values())[0]

    now = pd.Timestamp.now(tz="UTC")
    future = pd.DatetimeIndex([now + pd.Timedelta(hours=h) for h in (1, 2, 3)])
    source = app.connectors.get("remote")
    seed_resources = Channels([channel.duplicate(id=f"{channel.id}.4001", creation=4001)])
    source.connect(seed_resources)
    frame = pd.DataFrame(index=future)
    frame[f"{channel.id}.4001"] = 4001.0
    source.write(frame)
    source.disconnect()

    mirror._mirror_once()
    target = app.connectors.get("local")
    with target.engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM mirror")).scalar() == 3

    # Cancel the middle future record in the source; the next run must remove it from the target.
    with source.engine.begin() as connection:
        connection.execute(
            text("DELETE FROM mirror WHERE value = 4001.0 AND logged = :ts"),
            {"ts": future[1].tz_localize(None).to_pydatetime()},
        )
    mirror._mirror_once()

    with target.engine.connect() as connection:
        rows = connection.execute(text("SELECT COUNT(*) FROM mirror")).scalar()
    assert rows == 2


def test_copy_in_flight_skips_next_run():
    _register_sqlite_mirror_database()
    app = _build_application(tempfile.mkdtemp(prefix="mirror_overlap_"))

    mirror = app.components["sched"]
    mirror.data.add("value", table="mirror", column="value", type="float")

    assert mirror._copy_lock.acquire(blocking=False)
    try:
        mirror._mirror_once()
        # The run skipped before resolving or connecting the databases.
        assert app.connectors.get("local").engine is None
        assert app.connectors.get("remote").engine is None
    finally:
        mirror._copy_lock.release()

    # With the lock released the same call proceeds into the copy (and connects the source).
    mirror._mirror_once()
    assert app.connectors.get("remote").engine is not None


def test_empty_channel_set_is_a_noop():
    _register_sqlite_mirror_database()
    app = _build_application(tempfile.mkdtemp(prefix="mirror_empty_"))

    mirror = app.components["sched"]
    # No channels declared: _mirror_once returns before resolving or connecting the databases.
    mirror._mirror_once()

    assert app.connectors.get("local").engine is None
    assert app.connectors.get("remote").engine is None
