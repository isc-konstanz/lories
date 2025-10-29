# -*- coding: utf-8 -*-
"""
lories.application.view.pages.components.weather
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import List

import dash_bootstrap_components as dbc
import plotly.graph_objs as go
from dash import Input, Output, callback, dcc, html

import pandas as pd
import pytz as tz
from lories.application.view.pages import ComponentPage, PageLayout, register_component_group, register_component_page
from lories.components.weather import WeatherForecast as Forecast
from lories.components.weather import WeatherProvider as Weather
from lories.util import floor_date


@register_component_page(Weather)
@register_component_group(Weather, name="Weather")
class WeatherPage(ComponentPage[Weather]):
    # noinspection PyProtectedMember
    @property
    def forecast(self) -> Forecast:
        return self._component.forecast

    def create_layout(self, layout: PageLayout) -> None:
        super().create_layout(layout)

        current = self._build_current()
        layout.card.append(current, focus=True)
        layout.append(dbc.Row(dbc.Col(current, width="auto")))

        if self.forecast.is_enabled():
            forecast = self._build_forecast()
            layout.card.append(forecast)
            layout.append(dbc.Row(dbc.Col(forecast, width="auto")))

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
            Output(f"{self.id}-forecast-icons", "children"),
            Input(f"{self.id}-forecast-update", "n_intervals"),
        )
        def _update_forecast(*_):
            start = floor_date(pd.Timestamp.now(tz.UTC), freq="h")
            end = start + pd.Timedelta(hours=23)
            forecast = self.forecast.get(start, end)
            if forecast.empty:
                forecast = pd.DataFrame(index=pd.date_range(start, end, freq="h"), columns=["icon", Weather.TEMP_AIR])

            return self._build_forecast_graph(forecast), self._build_forecast_icons(forecast)

        return html.Div(
            [
                dbc.Row(id=f"{self.id}-forecast-icons"),
                dbc.Row(
                    dbc.Col(
                        [
                            dcc.Graph(id=f"{self.id}-forecast-graph", config={"displayModeBar": False}),
                            dcc.Interval(id=f"{self.id}-forecast-update", interval=60000, n_intervals=0),
                        ]
                    )
                ),
            ],
            id=f"{self.id}-forecast",
            # className="weather-forecast",
            # TODO: Move this to separate style sheet
            style={
                "padding-top": "1rem",
                "padding-bottom": "1rem",
                "background-image": "linear-gradient(#91d0ff, #c2e4ff)",
                "background-repeat": "no-repeat",
                # "border-radius": "var(--bs-border-radius)",
                # Background should stop above padding x axis margin (32px)
                "background-size": "100% calc(100% - 32px - 1rem)",
            },
        )

    def _build_forecast_graph(self, forecast: pd.DataFrame):
        temperature = forecast[Weather.TEMP_AIR]

        # Create the graph trace
        data = go.Scatter(
            x=temperature.index,
            y=temperature.values,
            mode="lines",
            line={"color": "#e4002c"},
        )

        ticks = self._slice_forecast_index(forecast)

        # Create the graph layout
        layout = go.Layout(
            xaxis=go.layout.XAxis(
                visible=True,
                range=[min(temperature.index), max(temperature.index)],
                tickmode="array",
                tickvals=ticks,
                ticktext=[t.strftime("%H:%M") for t in ticks],
            ),
            yaxis=go.layout.YAxis(
                visible=False,
                range=[
                    round(min(temperature.values) - 10),
                    round(max(temperature.values) + 10),
                ],
            ),
            margin=go.layout.Margin(l=0, r=0, b=32, t=12),
            height=250,
            dragmode=False,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        return {"data": [data], "layout": layout}

    def _build_forecast_icons(self, forecast: pd.DataFrame):
        icons = []
        for timestamp in self._slice_forecast_index(forecast):
            icon = forecast.loc[timestamp, "icon"]
            if not pd.isna(icon):
                icons.append(dbc.Col(html.Div(icon, style={"fontSize": ".75rem", "text-align": "center"})))
            else:
                icons.append(dbc.Col(dbc.Spinner()))
        return icons

    @staticmethod
    def _slice_forecast_index(forecast: pd.DataFrame, hours: int = 4) -> pd.DatetimeIndex:
        times = []
        time = floor_date(forecast.index[0], freq=f"{hours}h")
        while time <= forecast.index[-1]:
            if time in forecast.index:
                times.append(time)
            time += pd.Timedelta(hours=hours)
        return pd.DatetimeIndex(times)
