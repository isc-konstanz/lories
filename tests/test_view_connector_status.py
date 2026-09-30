# -*- coding: utf-8 -*-
"""
tests.test_view_connector_status
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The component and connector pages show a connector's status from its lifecycle flag and
timestamps. They never call ``is_connected()``, which does network I/O for SQL and InfluxDB.
"""

from __future__ import annotations

import pytest

import pandas as pd

pytest.importorskip("dash")

from dash._utils import to_json  # noqa: E402

from lories.application.view.pages.components.page import ComponentPage  # noqa: E402
from lories.application.view.pages.connectors.page import ConnectorPage  # noqa: E402


class _Connector:
    id = "system.database"
    key = "database"
    name = "Database"

    def __init__(self, connected: bool) -> None:
        self._connected = connected
        self._timestamp_connect = pd.Timestamp("2026-09-30 12:00:00", tz="UTC") if connected else pd.NaT
        self._timestamp_disconnect = pd.NaT if connected else pd.Timestamp("2026-09-30 12:05:00", tz="UTC")

    @staticmethod
    def is_enabled() -> bool:
        return True

    def is_connected(self) -> bool:
        raise AssertionError("page probed the connection")

    def _is_connected(self) -> bool:
        raise AssertionError("page probed the connection")


_STATUS = [(True, '"Connected"', "12:00:00"), (False, '"Disconnected"', "12:05:00")]


@pytest.mark.parametrize("connected, label, since", _STATUS)
def test_component_page_connector_status(connected, label, since):
    page = object.__new__(ComponentPage)
    page.id = "status"
    status = to_json(page._build_connector_item(_Connector(connected)))
    assert label in status and since in status


@pytest.mark.parametrize("connected, label, since", _STATUS)
def test_connector_page_status(connected, label, since):
    page = object.__new__(ConnectorPage)
    page.id = f"status-{connected}"
    page._connector = _Connector(connected)
    status = to_json(page._build_status())
    assert label in status and since in status
