# -*- coding: utf-8 -*-
"""
tests.test_view_weather
~~~~~~~~~~~~~~~~~~~~~~~

The weather page plots seven days of measured and three days of forecast weather in one chart.
"""

from __future__ import annotations

import itertools
from types import SimpleNamespace

import pytest

import pandas as pd
import pytz

dash = pytest.importorskip("dash")

from lories.application.view.pages.components.weather import WeatherPage  # noqa: E402

_pages = itertools.count()
_TZ = pytz.timezone("Europe/Berlin")


def _measured(start: pd.Timestamp, hours: int = 3) -> pd.DataFrame:
    index = pd.date_range(start, periods=hours * 60, freq="1min", tz="UTC")
    return pd.DataFrame({"ghi": 100.0, "temp_air": -2.0, "precipitation": 0.5}, index=index)


def _forecast(start: pd.Timestamp, hours: int = 6, **columns) -> pd.DataFrame:
    index = pd.date_range(start, periods=hours, freq="h", tz=_TZ)
    return pd.DataFrame(columns or {"ghi": 50.0, "temp_air": 1.0, "precipitation": 1.0}, index=index)


def test_full_figure():
    now = pd.Timestamp("2026-10-12 12:00", tz=_TZ)
    start = pd.Timestamp("2026-10-05 12:00", tz=_TZ)
    end = pd.Timestamp("2026-10-15 12:00", tz=_TZ)
    first = pd.Timestamp("2026-10-12 06:00", tz="UTC")

    figure = WeatherPage._build_forecast_graph(
        _measured(first), _forecast(pd.Timestamp("2026-10-12 13:00", tz=_TZ)), now, start, end
    )
    traces = figure["data"]

    assert [t.name for t in traces] == [
        "Irradiance",
        "Temperature",
        "Precipitation",
        "Irradiance forecast",
        "Temperature forecast",
        "Precipitation forecast",
    ]
    assert [t.yaxis for t in traces] == ["y", "y3", "y2", "y", "y3", "y2"]
    assert [t.type for t in traces] == ["scatter", "scatter", "bar", "scatter", "scatter", "bar"]
    assert [t.line.dash for t in traces if t.type == "scatter"] == [None, None, "dash", "dash"]
    assert traces[0].fill == "tozeroy"
    assert traces[0].x[0] == first.tz_convert(_TZ)

    steps = pd.Series(list(traces[2].x)).diff().dropna()
    assert (steps == pd.Timedelta(minutes=30)).all()

    layout = figure["layout"]
    assert layout["yaxis3"].get("rangemode") != "tozero"
    assert list(layout["xaxis"]["range"]) == [start, end]
    assert len(layout["shapes"]) == 1
    assert layout["shapes"][0]["x0"] == now


def test_missing_data():
    now = pd.Timestamp("2026-10-12 12:00", tz=_TZ)
    start = now - pd.Timedelta(days=7)
    end = now + pd.Timedelta(days=3)

    forecast = _forecast(now, temp_air=3.0)
    figure = WeatherPage._build_forecast_graph(pd.DataFrame(), forecast, now, start, end)
    assert len(figure["data"]) == 1
    assert figure["data"][0].yaxis == "y3"
    assert figure["data"][0].line.dash == "dash"

    figure = WeatherPage._build_forecast_graph(pd.DataFrame(), pd.DataFrame(), now, start, end)
    assert len(figure["data"]) == 0
    assert len(figure["layout"]["shapes"]) == 1

    empty = pd.DataFrame(columns=["ghi", "temp_air", "precipitation"])
    figure = WeatherPage._build_forecast_graph(empty, empty, now, start, end)
    assert len(figure["data"]) == 0


def test_callback_reads_week_and_horizon():
    calls = {}

    def from_logger(channels, start=None, end=None):
        calls["logger"] = (start, end)
        return _measured(pd.Timestamp.now(tz="UTC").floor("h") - pd.Timedelta(hours=3))

    def get(start, end):
        calls["forecast"] = (start, end)
        return _forecast(start.floor("h") + pd.Timedelta(hours=1))

    channels = [SimpleNamespace(key=key) for key in ("ghi", "temp_air", "precipitation", "other")]
    data = SimpleNamespace(
        filter=lambda fn: [c for c in channels if fn(c)],
        from_logger=from_logger,
    )
    page = object.__new__(WeatherPage)
    page.id = f"weather-{next(_pages)}"
    page._component = SimpleNamespace(
        location=SimpleNamespace(timezone=_TZ),
        data=data,
        forecast=SimpleNamespace(get=get),
    )
    body = page._build_forecast()

    app = dash.Dash(__name__)
    app.layout = dash.html.Div([body])
    client = app.server.test_client()
    client.get("/_dash-layout")

    graph = f"{page.id}-forecast-graph"
    update = f"{page.id}-forecast-update"
    key = next(key for key in app.callback_map if graph in key)
    response = client.post(
        "/_dash-update-component",
        json={
            "output": key,
            "outputs": {"id": graph, "property": "figure"},
            "inputs": [{"id": update, "property": "n_intervals", "value": 1}],
            "changedPropIds": [f"{update}.n_intervals"],
        },
    )
    assert response.status_code == 200, response.data
    figure = response.get_json()["response"][graph]["figure"]
    assert len(figure["data"]) > 0

    start, end = calls["logger"]
    assert abs((end - start) - pd.Timedelta(days=7)) <= pd.Timedelta(hours=1)
    start, end = calls["forecast"]
    assert abs((end - start) - pd.Timedelta(days=3)) <= pd.Timedelta(hours=1)
