# -*- coding: utf-8 -*-
"""
End-to-end integration test for ``Databases.rotate``.

Drives the real rotation pipeline (``Application`` -> ``Databases.rotate`` ->
``Retention.aggregate``) against an in-memory ``Database`` and asserts that

1. raw rows older than the ``rotate`` cutoff are deleted in place, and
2. rows in the retained-but-aged window are downsampled in place (fine rows
   replaced by fewer coarse rows written back to the same store).

On the unfixed ``databases.py`` (which selects channels via the phantom
``c.rotation.database.id``) this test fails with an ``AttributeError`` before
any data is touched, so it genuinely exercises the rotation bug fix.
"""

from __future__ import annotations

import os
import tempfile
from typing import Optional

import pytest

import pandas as pd

pytestmark = pytest.mark.slow


def _build_memory_database_cls():
    """Register and return an in-memory ``Database`` connector type.

    The optional-dependency-free imports live inside the test so collection
    never depends on connector internals being importable at module load.
    """
    from lories.connectors import Database, register_connector_type
    from lories.typing import Resources, Timestamp  # noqa: F401

    @register_connector_type("memory", replace=True)
    class MemoryDatabase(Database):
        """A ``Database`` whose entire store is a single in-RAM DataFrame.

        ``csv`` has no ``delete`` and ``virtual`` is not a ``Database``, so a
        custom subclass is the only logger DB that supports the full rotate
        cycle (read_first/read_last/read/delete/write) without touching disk.
        """

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self._store = pd.DataFrame()

        def _columns(self, resources) -> list:
            return [r.id for r in resources if r.id in self._store.columns]

        def connect(self, resources) -> None:
            pass

        def disconnect(self) -> None:
            pass

        def is_connected(self) -> bool:
            return True

        def read(self, resources, start=None, end=None) -> pd.DataFrame:
            data = self._store.loc[:, self._columns(resources)].copy()
            if start is not None:
                data = data[data.index >= start]
            if end is not None:
                data = data[data.index <= end]
            return data

        def read_first(self, resources) -> Optional[pd.DataFrame]:
            columns = self._columns(resources)
            if self._store.empty or len(columns) == 0:
                return None
            return self._store.loc[:, columns].head(1).copy()

        def read_last(self, resources) -> Optional[pd.DataFrame]:
            columns = self._columns(resources)
            if self._store.empty or len(columns) == 0:
                return None
            return self._store.loc[:, columns].tail(1).copy()

        def write(self, data: pd.DataFrame) -> None:
            self._store = data.combine_first(self._store).sort_index()

        def delete(self, resources, start=None, end=None) -> None:
            columns = self._columns(resources)
            if self._store.empty or len(columns) == 0:
                return
            mask = pd.Series(True, index=self._store.index)
            if start is not None:
                mask &= self._store.index >= start
            if end is not None:
                mask &= self._store.index <= end
            self._store = self._store.loc[~mask]

    return MemoryDatabase


def _build_application(tmp_dir: str):
    """Boot a real ``Application`` with a single in-memory database connector.

    Channels are deliberately omitted from the configuration: converters load
    after channels during ``configure`` (so an inline typed channel cannot be
    built at boot), and ``rotate`` takes the channels as an argument anyway.
    """
    from lories.application import Settings
    from lories.application.main import Application

    conf_dir = os.path.join(tmp_dir, "conf")
    data_dir = os.path.join(tmp_dir, "data")
    os.makedirs(conf_dir)
    os.makedirs(data_dir)

    settings = (
        'name = "rotate_it"\n'
        'action = "rotate"\n'
        "\n"
        "[interface]\n"
        "enabled = false\n"
        "\n"
        "[connectors.memdb]\n"
        'type = "memory"\n'
        'timezone = "UTC"\n'
    )
    with open(os.path.join(conf_dir, "settings.conf"), "w") as file:
        file.write(settings)

    cwd = os.getcwd()
    os.chdir(tmp_dir)
    try:
        settings_obj = Settings("rotate_it")
        app = Application(settings_obj)
        app.configure(settings_obj)
    finally:
        os.chdir(cwd)
    return app


def test_rotate_deletes_and_downsamples_in_place():
    from lories.core import Configurations
    from lories.data.channels import Channels
    from lories.data.databases import Databases
    from lories.data.retention import Retention
    from lories.util import floor_date, to_timedelta

    _build_memory_database_cls()

    tmp_dir = tempfile.mkdtemp(prefix="rotate_it_")
    app = _build_application(tmp_dir)

    database = app.connectors.get("memdb")
    assert database is not None

    channel = app._create(
        id="power",
        key="power",
        type="float",
        logger={"connector": "memdb"},
        rotate="14D",
        aggregate="mean",
        retention={"7D": {"keep": "7D", "freq": "1D", "resample": "1h"}},
    )
    assert channel.has_logger("memdb")
    channels = Channels([channel])

    # Seed 30 days of fine-grained (1-minute) tz-aware UTC data.
    now = pd.Timestamp.now(tz="UTC").floor("min")
    index = pd.date_range(end=now, periods=30 * 24 * 60, freq="1min", tz="UTC")
    database._store = pd.DataFrame({"power": range(len(index))}, index=index, dtype=float)

    seeded_rows = len(database._store)
    seeded_min = database._store.index.min()

    # Cutoffs the pipeline uses, recomputed here for assertions (UTC, day floor).
    rotate_cutoff = floor_date(pd.Timestamp.now(tz="UTC") - to_timedelta("14D"), freq="D")
    retain_cutoff = floor_date(pd.Timestamp.now(tz="UTC") - to_timedelta("7D"), freq="D")

    # Build the Databases context exactly as Application.rotate does.
    defaults = app.configs.get_member(Retention.TYPE, defaults={})
    rotate_configs = Configurations(f"{Retention.TYPE}.conf", app.configs.dirs, defaults=defaults)
    rotate_configs._load(require=False)
    databases = Databases(app, rotate_configs)
    assert "memdb" in databases.keys()

    databases.rotate(channels)

    store = database._store
    assert not store.empty

    # (1) Rotation: every raw row older than the rotate cutoff is gone.
    assert store.index.min() >= rotate_cutoff
    assert seeded_min < rotate_cutoff  # sanity: there *were* rows to delete
    assert len(store) < seeded_rows

    # (2) Retention: fine rows in the retained-but-aged window were replaced by
    #     fewer coarse (hourly) rows written back to the *same* store.
    gaps_minutes = set((store.index.to_series().diff().dropna().dt.total_seconds() / 60).round().astype(int))
    assert 60 in gaps_minutes, gaps_minutes  # coarse hourly rows are present
    assert 1 in gaps_minutes, gaps_minutes  # recent (< keep) data stays fine

    # Coarse hourly timestamps land in the [rotate_cutoff, retain_cutoff] window.
    aged = store[(store.index >= rotate_cutoff) & (store.index < retain_cutoff)]
    assert not aged.empty
    on_the_hour = aged.index[(aged.index.minute == 0) & (aged.index.second == 0)]
    assert len(on_the_hour) > 0
    # The aged window is hourly: far fewer rows than the 1440/day it was seeded with.
    aged_days = (retain_cutoff - rotate_cutoff).days
    assert len(aged) <= aged_days * 24 + 24
