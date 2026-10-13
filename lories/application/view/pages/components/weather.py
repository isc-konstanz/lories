# -*- coding: utf-8 -*-
"""
lories.application.view.pages.components.weather
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import List, Optional

import dash_bootstrap_components as dbc
import plotly.graph_objs as go
from dash import Input, Output, callback, dcc, html

import pandas as pd
from lories.application.view.pages import ComponentPage, PageLayout, register_component_page
from lories.components.weather import Weather, WeatherForecast
from lories.util import floor_date

HISTORY = pd.Timedelta(days=7)
HORIZON = pd.Timedelta(days=3)

COLOR_IRRADIANCE = "#F2A900"
COLOR_TEMPERATURE = "#E45756"
COLOR_PRECIPITATION = "#4C78A8"
COLOR_PRECIPITATION_FORECAST = "#95BAE6"


@register_component_page(Weather)
class WeatherPage(ComponentPage[Weather]):
    @property
    def forecast(self) -> Optional[WeatherForecast]:
        return getattr(self._component, "forecast", None)

    def create_layout(self, layout: PageLayout) -> None:
        super().create_layout(layout)

        current = self._build_current()
        layout.card.append(current, focus=True)
        layout.append(dbc.Row(dbc.Col(current, width="auto")))

        forecast = self.forecast
        if forecast is not None and forecast.is_enabled():
            forecast_widget = self._build_forecast()
            layout.card.append(forecast_widget)
            layout.append(dbc.Row(dbc.Col(forecast_widget, width="auto")))

    def _build_current(self) -> html.Div:
        @callback(
            Output(f"{self.id}-current", "children"),
            Input(f"{self.id}-current-update", "n_intervals"),
        )
        def _update_current(*_) -> List[html.P] | dbc.Spinner:
            icon = self.data.icon
            if icon.is_valid():
                return [
                    html.P(icon.timestamp.strftime("Today, %d. %b %Y from %H:%M")),
                    html.P(str(icon.value).replace("-", " "), style={"fontSize": "4rem"}),
                ]
            return dbc.Spinner(html.Div(id=f"{self.id}-current-loader"))

        return html.Div(
            [
                html.Div(
                    _update_current(),
                    id=f"{self.id}-current",
                ),
                dcc.Interval(
                    id=f"{self.id}-current-update",
                    interval=60000,
                    n_intervals=0,
                ),
            ]
        )

    def _build_forecast(self) -> html.Div:
        @callback(
            Output(f"{self.id}-forecast-graph", "figure"),
            Input(f"{self.id}-forecast-update", "n_intervals"),
        )
        def _update_forecast(*_):
            now = pd.Timestamp.now(tz=self._component.location.timezone)
            start = floor_date(now - HISTORY, freq="h")
            end = floor_date(now + HORIZON, freq="h")
            measured = self._read_measured(start, now)
            forecast = self.forecast.get(now, end)
            return self._build_forecast_graph(measured, forecast, now, start, end)

        return html.Div(
            [
                dcc.Graph(id=f"{self.id}-forecast-graph", config={"displayModeBar": False}),
                dcc.Interval(id=f"{self.id}-forecast-update", interval=300000, n_intervals=0),
            ],
            id=f"{self.id}-forecast",
        )

    def _read_measured(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        channels = self.data.filter(lambda c: c.key in (Weather.GHI, Weather.TEMP_AIR, Weather.PRECIPITATION))
        if len(channels) == 0:
            return pd.DataFrame()
        return self.data.from_logger(channels, start=start, end=end)

    @staticmethod
    def _build_forecast_graph(
        measured: pd.DataFrame,
        forecast: pd.DataFrame,
        now: pd.Timestamp,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> dict:
        # plotly.js drops UTC offsets, so all frames must share one timezone to line up
        frames = []
        for frame in (measured, forecast):
            if not frame.empty:
                frame = frame.tz_convert(now.tz)
            frames.append(frame)
        measured, forecast = frames

        def resampled(frame: pd.DataFrame, key: str, freq: str) -> pd.Series:
            if frame.empty or key not in frame.columns:
                return pd.Series(dtype=float)
            return frame[key].resample(freq).mean().dropna()

        data = []
        for frame, is_forecast in ((measured, False), (forecast, True)):
            suffix = " forecast" if is_forecast else ""
            dash = "dash" if is_forecast else None

            series = resampled(frame, Weather.GHI, "15min")
            if not series.empty:
                data.append(
                    go.Scatter(
                        x=list(series.index),
                        y=series.values,
                        name=f"Irradiance{suffix}",
                        yaxis="y",
                        mode="lines",
                        fill="tozeroy",
                        line=dict(color=COLOR_IRRADIANCE, dash=dash),
                    )
                )
            series = resampled(frame, Weather.TEMP_AIR, "15min")
            if not series.empty:
                data.append(
                    go.Scatter(
                        x=list(series.index),
                        y=series.values,
                        name=f"Temperature{suffix}",
                        yaxis="y3",
                        mode="lines",
                        line=dict(color=COLOR_TEMPERATURE, dash=dash),
                    )
                )
            series = resampled(frame, Weather.PRECIPITATION, "30min")
            if not series.empty:
                data.append(
                    go.Bar(
                        x=list(series.index),
                        y=series.values,
                        name=f"Precipitation{suffix}",
                        yaxis="y2",
                        marker=dict(color=COLOR_PRECIPITATION_FORECAST if is_forecast else COLOR_PRECIPITATION),
                    )
                )

        layout = dict(
            template="plotly_white",
            hovermode="x unified",
            showlegend=False,
            height=400,
            dragmode=False,
            margin=dict(l=60, r=20, t=20, b=40),
            xaxis=dict(domain=[0, 0.88], range=[start, end]),
            yaxis=dict(
                title=dict(text="Irradiance [W/m²]", font=dict(color=COLOR_IRRADIANCE)),
                tickfont=dict(color=COLOR_IRRADIANCE),
                rangemode="tozero",
            ),
            yaxis3=dict(
                title=dict(text="Temperature [°C]", font=dict(color=COLOR_TEMPERATURE)),
                tickfont=dict(color=COLOR_TEMPERATURE),
                overlaying="y",
                side="right",
                showgrid=False,
            ),
            yaxis2=dict(
                title=dict(text="Precipitation [mm/h]", font=dict(color=COLOR_PRECIPITATION)),
                tickfont=dict(color=COLOR_PRECIPITATION),
                overlaying="y",
                side="right",
                anchor="free",
                position=0.96,
                rangemode="tozero",
                showgrid=False,
            ),
            shapes=[
                dict(
                    type="line",
                    xref="x",
                    yref="paper",
                    x0=now,
                    x1=now,
                    y0=0,
                    y1=1,
                    line=dict(color="#888888", width=1, dash="dot"),
                )
            ],
        )
        return {"data": data, "layout": layout}
