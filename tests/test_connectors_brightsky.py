# -*- coding: utf-8 -*-
"""
tests.test_connectors_brightsky
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Reads of the Bright Sky connector against a fake API that applies the real date rules: ``date`` and ``last_date``
are both inclusive, a bare date means 00:00 in the ``tz`` parameter, and without ``tz`` the offset of ``date``
applies, else UTC.
"""

from __future__ import annotations

import json
import logging
import socket
import threading
from types import SimpleNamespace
from typing import Optional

import pytest
import requests

import pandas as pd
from lories.components.weather.dwd import brightsky
from lories.components.weather.dwd.brightsky import Brightsky
from lories.connectors import ConnectorError
from lories.core import Resource, Resources
from lories.location import Location

CURRENT = 1
FORECAST = 2


def _bound(value: str, tz: Optional[str]) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(tz or "UTC")
    return timestamp


class FakeBrightSky:
    def __init__(
        self,
        observed_until: Optional[pd.Timestamp] = None,
        missing: tuple = (),
        solar: float = 0.0,
        error: Optional[Exception] = None,
        status: int = 200,
        body: Optional[str] = None,
    ) -> None:
        self.observed_until = observed_until
        self.missing = [pd.Timestamp(t) for t in missing]
        self.solar = solar
        self.error = error
        self.status = status
        self.body = body

    def get(self, url, params, **kwargs):
        if self.error is not None:
            raise self.error
        if self.status != 200:
            return SimpleNamespace(status_code=self.status, reason="Service Unavailable", text="")
        if self.body is not None:
            return SimpleNamespace(status_code=200, reason="OK", text=self.body)
        params = {k: v for k, v in params.items() if v is not None}
        tz = params.get("tz")
        first = _bound(params["date"], tz)
        last = _bound(params["last_date"], tz) if "last_date" in params else first + pd.Timedelta(days=1)
        hours = [t for t in pd.date_range(first.ceil("h"), last.floor("h"), freq="h") if t not in self.missing]
        weather = [
            {
                "timestamp": t.tz_convert(tz or "UTC").isoformat(),
                "source_id": self._source(t),
                "precipitation": 1.0,
                "solar": self.solar,
                "cloud_cover": 50,
            }
            for t in hours
        ]
        return SimpleNamespace(
            status_code=200, reason="OK", text=json.dumps({"weather": weather, "sources": self._sources()})
        )

    def _source(self, timestamp: pd.Timestamp) -> int:
        if self.observed_until is None or timestamp <= self.observed_until:
            return CURRENT
        return FORECAST

    def _sources(self) -> list[dict]:
        observed_until = self.observed_until or pd.Timestamp("2026-10-07 00:00Z")
        return [
            {
                "id": CURRENT,
                "observation_type": "current",
                "first_record": "2026-09-01T00:00:00+00:00",
                "last_record": observed_until.isoformat(),
            },
            {
                "id": FORECAST,
                "observation_type": "forecast",
                "first_record": (observed_until + pd.Timedelta(hours=1)).isoformat(),
                "last_record": (observed_until + pd.Timedelta(days=10)).isoformat(),
            },
        ]


def _install(monkeypatch, fake=None) -> FakeBrightSky:
    fake = fake or FakeBrightSky()
    monkeypatch.setattr(brightsky.requests, "get", fake.get)
    return fake


def _connector(timezone="Europe/Berlin") -> Brightsky:
    connector = Brightsky.__new__(Brightsky)
    connector._id = "weather.brightsky"
    connector._logger = logging.getLogger("test_connectors_brightsky")
    connector.address = "https://api.brightsky.dev/"
    connector.horizon = 10
    connector._timeout = pd.Timedelta("30s")
    connector.location = Location(47.766881, 9.556031, timezone=timezone)
    return connector


def _resources(key: str = "precipitation", address: str = "precipitation", source: str = "current, historical"):
    return Resources([Resource(id=f"weather.{key}", key=key, type=float, address=address, source=source)])


def _read(connector: Brightsky, resources=None, start: Optional[str] = None, end: Optional[str] = None):
    start = pd.Timestamp(start) if start is not None else None
    end = pd.Timestamp(end) if end is not None else None
    return connector.read(resources or _resources(), start, end)


def _utc(data: pd.DataFrame) -> list[pd.Timestamp]:
    if data.empty:
        return []
    return list(data.index.tz_convert("UTC"))


def test_utc_tick_window_inside_one_day_returns_its_hourly_record(monkeypatch):
    _install(monkeypatch)
    data = _read(_connector(), start="2026-10-05 07:50Z", end="2026-10-05 08:20Z")

    assert _utc(data) == [pd.Timestamp("2026-10-05 08:00Z")]


def test_utc_tick_window_late_in_the_utc_day_returns_its_hourly_record(monkeypatch):
    _install(monkeypatch)
    data = _read(_connector(), start="2026-10-05 22:50Z", end="2026-10-05 23:20Z")

    assert _utc(data) == [pd.Timestamp("2026-10-05 23:00Z")]


def test_utc_window_ending_at_utc_midnight_returns_the_last_hours_of_the_utc_day(monkeypatch):
    _install(monkeypatch)
    data = _read(_connector(), start="2026-10-04 22:30Z", end="2026-10-05 00:00Z")

    assert _utc(data) == [pd.Timestamp("2026-10-04 23:00Z"), pd.Timestamp("2026-10-05 00:00Z")]


@pytest.mark.parametrize(
    "fake",
    [
        FakeBrightSky(error=requests.ConnectionError("unreachable")),
        FakeBrightSky(status=503),
        FakeBrightSky(body="<html>Bad Gateway</html>"),
    ],
    ids=["unreachable", "http-error", "not-json"],
)
def test_failing_api_raises_connector_error(monkeypatch, fake):
    _install(monkeypatch, fake)

    with pytest.raises(ConnectorError):
        _read(_connector(), start="2026-10-05 07:50Z", end="2026-10-05 08:20Z")


def test_stalled_api_times_out_with_connector_error():
    errors = []
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        connector = _connector()
        connector.address = f"http://127.0.0.1:{server.getsockname()[1]}/"
        connector._timeout = pd.Timedelta("0.2s")

        def read():
            try:
                _read(connector, start="2026-10-05 07:50Z", end="2026-10-05 08:20Z")
            except Exception as e:
                errors.append(e)

        thread = threading.Thread(target=read, daemon=True)
        thread.start()
        thread.join(timeout=5)

        assert not thread.is_alive()
    assert isinstance(errors[0], ConnectorError)


def test_read_without_matching_observations_returns_empty_frame(monkeypatch):
    _install(monkeypatch, FakeBrightSky(observed_until=pd.Timestamp("2026-10-01 00:00Z")))
    data = _read(_connector(), start="2026-10-05 07:50Z", end="2026-10-05 08:20Z")

    assert data.empty


def test_latest_read_before_todays_first_observation_returns_yesterdays_last(monkeypatch):
    last_observation = pd.Timestamp.now(tz="Europe/Berlin").normalize() - pd.Timedelta(hours=1)
    _install(monkeypatch, FakeBrightSky(observed_until=last_observation))
    data = _read(_connector())

    assert _utc(data) == [last_observation.tz_convert("UTC")]


def test_ghi_is_the_irradiation_of_the_previous_hour_also_after_a_missing_record(monkeypatch):
    _install(monkeypatch, FakeBrightSky(missing=("2026-10-05 08:00Z",), solar=0.5))
    data = _read(_connector(), _resources("ghi", "solar"), start="2026-10-05 06:30Z", end="2026-10-05 10:30Z")

    assert data["weather.ghi"].tolist() == [500.0, 500.0, 500.0]


@pytest.mark.parametrize("timezone", [2, "+02:00"], ids=["hours", "offset-string"])
def test_fixed_offset_site_timezone_returns_the_hourly_record(monkeypatch, timezone):
    _install(monkeypatch)
    data = _read(_connector(timezone), start="2026-10-05 22:50Z", end="2026-10-05 23:20Z")

    assert _utc(data) == [pd.Timestamp("2026-10-05 23:00Z")]


def test_ranged_forecast_read_returns_only_the_window(monkeypatch):
    _install(monkeypatch, FakeBrightSky(observed_until=pd.Timestamp("2026-10-05 00:00Z")))
    data = _read(_connector(), _resources(source="forecast"), start="2026-10-05 07:50Z", end="2026-10-05 10:20Z")

    assert _utc(data) == [pd.Timestamp(f"2026-10-05 {hour}:00Z") for hour in ("08", "09", "10")]
