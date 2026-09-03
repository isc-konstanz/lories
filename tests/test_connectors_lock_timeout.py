# -*- coding: utf-8 -*-
"""
lories.tests.test_connectors_lock_timeout
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A timed-out lock acquire in the ``_do_*`` wrappers must raise the intended
``ConnectorError`` and must not touch the lock. Before the fix the acquire
lived inside the ``try``, so the ``finally`` released a lock the timed-out
thread never held -- silently stealing it from the thread that does hold it
(or raising a bare ``RuntimeError`` when it was unheld). Tracked as
lories-frictions issue 06.
"""

from __future__ import annotations

from threading import Lock

import pytest

import pandas as pd
from lories.connectors.connector import Connector
from lories.connectors.errors import ConnectorError
from lories.data.database import Database


class _FakeConnector(Connector):
    id = "lock_probe"

    def read(self, resources):
        return pd.DataFrame()

    def write(self, data):
        pass


class _FakeDatabase(Database):
    id = "lock_probe_db"

    def read(self, resources, start=None, end=None):
        return pd.DataFrame()

    def read_first(self, resources):
        return None

    def read_last(self, resources):
        return None

    def write(self, data):
        pass


def _probe(cls):
    probe = cls.__new__(cls)
    probe._lock = Lock()
    probe._lock_timeout = 0.05
    return probe


CONNECTOR_CALLS = [
    ("_do_connect", (None,), "connecting"),
    ("_do_disconnect", (), "disconnecting"),
    ("_do_read", (None,), "reading"),
    ("_do_write", (None,), "writing"),
]

DATABASE_CALLS = [
    ("_do_hash", (None,), "hashing"),
    ("_do_exists", (None,), "checking existence of"),
    ("_do_read", (None,), "reading"),
    ("_do_read_first", (None,), "reading first values of"),
    ("_do_read_first_index", (None,), "reading first index of"),
    ("_do_read_last", (None,), "reading last values of"),
    ("_do_read_last_index", (None,), "reading last index of"),
    ("_do_delete", (None,), "deleting data of"),
]


@pytest.mark.parametrize("method,args,operation", CONNECTOR_CALLS)
def test_connector_lock_timeout_raises_connector_error(method, args, operation):
    probe = _probe(_FakeConnector)
    assert probe._lock.acquire()
    try:
        with pytest.raises(ConnectorError, match=f"Timeout acquiring lock for {operation}"):
            getattr(probe, method)(*args)
        # The holder's lock must survive the timed-out call untouched
        assert probe._lock.locked()
    finally:
        probe._lock.release()


@pytest.mark.parametrize("method,args,operation", DATABASE_CALLS)
def test_database_lock_timeout_raises_connector_error(method, args, operation):
    probe = _probe(_FakeDatabase)
    assert probe._lock.acquire()
    try:
        with pytest.raises(ConnectorError, match=f"Timeout acquiring lock for {operation}"):
            getattr(probe, method)(*args)
        assert probe._lock.locked()
    finally:
        probe._lock.release()


def test_lock_released_after_successful_call():
    probe = _probe(_FakeConnector)
    probe._do_disconnect()
    assert not probe._lock.locked()
    assert probe._lock.acquire(blocking=False)
    probe._lock.release()
