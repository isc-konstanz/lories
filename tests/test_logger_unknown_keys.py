# -*- coding: utf-8 -*-
"""
tests.test_logger_unknown_keys
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Checks that keys under a channel's ``logger`` table, which neither the framework nor the serving
database declare, are reported once at WARNING, while the keys real configs use stay silent.
"""

from __future__ import annotations

import importlib
import logging
import os
import tempfile

import pytest

import pandas as pd

LOGGER_NAME = __name__


class _Records(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def messages(self, level: int) -> list:
        return [r.getMessage() for r in self.records if r.levelno == level]


@pytest.fixture
def records():
    handler = _Records()
    logger = logging.getLogger(LOGGER_NAME)
    previous = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    yield handler
    logger.removeHandler(handler)
    logger.setLevel(previous)


@pytest.fixture
def app():
    from lories.application import Settings
    from lories.application.main import Application
    from lories.connectors import Database, register_connector_type
    from lories.core.configs.parameters import ChannelParameter

    @register_connector_type("memory_unknown_keys", replace=True)
    class MemoryDatabase(Database):
        table = ChannelParameter(type=str, required=False, desc="Table name")
        column = ChannelParameter(type=str, required=False, desc="Column name")

        def connect(self, resources) -> None:
            pass

        def disconnect(self) -> None:
            pass

        def is_connected(self) -> bool:
            return True

        def read(self, resources, start=None, end=None) -> pd.DataFrame:
            return pd.DataFrame()

        def read_first(self, resources):
            return None

        def read_last(self, resources):
            return None

        def write(self, data: pd.DataFrame) -> None:
            pass

        def delete(self, resources, start=None, end=None) -> None:
            pass

    tmp_dir = tempfile.mkdtemp(prefix="logger_unknown_keys_")
    conf_dir = os.path.join(tmp_dir, "conf")
    os.makedirs(conf_dir)
    os.makedirs(os.path.join(tmp_dir, "data"))
    with open(os.path.join(conf_dir, "settings.conf"), "w") as file:
        file.write(
            'name = "logger_unknown_keys"\n\n[interface]\nenabled = false\n\n'
            '[connectors.memdb]\ntype = "memory_unknown_keys"\n'
        )

    cwd = os.getcwd()
    os.chdir(tmp_dir)
    try:
        settings = Settings("logger_unknown_keys")
        application = Application(settings)
        application.configure(settings)
    finally:
        os.chdir(cwd)
    return application


def _connect(app, **logger) -> None:
    channel = app._create(id="power", key="power", type="float", logger={"connector": "memdb", **logger})
    app._add(channel)
    database = app.connectors.get("memdb")
    database._connected = False
    app.connectors.connect(channels=app.channels)


def test_misspelled_key_warns_once_naming_channel_key_and_class(app, records):
    _connect(app, tabel="power_log", table="power_log")

    messages = records.messages(logging.WARNING)
    assert len(messages) == 1
    assert "'power'" in messages[0]
    assert "tabel" in messages[0]
    assert "MemoryDatabase" in messages[0]


def test_keys_used_by_configs_do_not_warn(app, records):
    _connect(app, enabled=True, table="power_log", column="power", freq="1min", aggregate="mean")

    assert records.messages(logging.WARNING) == []


def test_reconnect_does_not_repeat_and_distinct_keys_warn_separately(app, records):
    _connect(app, tabel="power_log", colum="power")
    assert len(records.messages(logging.WARNING)) == 2

    database = app.connectors.get("memdb")
    database._connected = False
    app.connectors.connect(channels=app.channels)

    assert len(records.messages(logging.WARNING)) == 2


def test_connector_table_check_stays_at_debug(app, records):
    logging.getLogger(LOGGER_NAME).setLevel(logging.DEBUG)
    channel = app._create(id="power", key="power", type="float", connector={"connector": "memdb", "topik": "x"})
    app._add(channel)
    database = app.connectors.get("memdb")
    database._connected = False
    app.connectors.connect(channels=app.channels)

    assert records.messages(logging.WARNING) == []
    assert any("topik" in m for m in records.messages(logging.DEBUG))


@pytest.mark.parametrize(
    "module, cls, keys",
    [
        (
            "lories.connectors.sql.database",
            "SqlDatabase",
            ["column", "length", "primary", "default", "nullable", "foreign", "attribute", "timezone"],
        ),
        ("lories.connectors.influxdb1", "InfluxDB1", ["field", "column"]),
        ("lories.connectors.influxdb2", "InfluxDB2", ["field", "column"]),
    ],
)
def test_databases_declare_the_keys_they_read(module, cls, keys):
    try:
        connector = getattr(importlib.import_module(module), cls)
    except ImportError as e:
        pytest.skip(str(e))

    declared = {p._resolve_key(): p for p in connector.__channel_parameters__.values()}
    for key in keys:
        assert key in declared
        assert declared[key].required is False
