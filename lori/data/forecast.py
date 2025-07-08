# -*- coding: utf-8 -*-
"""
lori.data.forecast
~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
from filterpy.kalman import KalmanFilter # MIT licence

from lori import Configurations
from lori.components import Component, register_component_type
from lori.core import ResourceException
from lori.data import Channel, Channels
from lori.typing import TimestampType


def get_forecast(
        self,
        column: str,
        start: TimestampType,
        end: TimestampType,
        method: str,
) -> pd.DataFrame:
    # Define window of past 3 weeks
    start = pd.Timestamp(start).floor("h")
    _start = start - pd.Timedelta(weeks=20)
    _end = start

    # Todo: Remove this, it is only for testing
    # Shift forward if data is not available before this date
    while _start < pd.Timestamp("2016-06-01T00:00:00+02:00"):
        _start += pd.Timedelta(weeks=1)
        _end += pd.Timedelta(weeks=1)

    # Load the last 3 weeks of data
    last_3w_ts = self.data.from_logger([column], start=_start, end=_end)[column]

    if method == "persistence_3weeks":
        persistence_mean, persistence_std = persistence_3_week(start, last_3w_ts)

        last_3w_filtered_ts = apply_kalman_filter(last_3w_ts[_end-pd.Timedelta(days=1):_end])

        current_filtered = last_3w_filtered_ts.iloc[-1]

        deterministic = deterministic_forecast_model(
            current=current_filtered,
            forecast_values=persistence_mean,
            t_half=12,  # Default half-life of 12 hours
            dt=1  # Default time step of 1 hour
        )
        plot_model = False
        if plot_model == True:
            import matplotlib.pyplot as plt
            plt.figure(figsize=(12, 6))
            plt.plot(last_3w_ts.index, last_3w_ts, label='Last 3 Weeks', color='C0', alpha=0.5)
            plt.plot(last_3w_filtered_ts.index, last_3w_filtered_ts, label='Kalman Filtered', color='C0')
            plt.plot(persistence_mean.index, persistence_mean, label='Persistence Mean', color='C1', alpha=0.5)
            plt.plot(deterministic.index, deterministic, label='Deterministic Forecast', color='C1')
            plt.fill_between(
                deterministic.index,
                deterministic - persistence_std,
                deterministic + persistence_std,
                color='C1',
                alpha=0.2,
                label='Persistence Std'
            )

            plt.show()
            pass

        # resample last_3w_ts to 10 frequency

        last_3w_ts = last_3w_ts.resample("1h").mean()

        persistence_mean, persistence_std = persistence_3_week(start, last_3w_ts)


        deterministic = deterministic_forecast_model(
            current=current_filtered,
            forecast_values=persistence_mean,
            t_half=12,  # Default half-life of 12 hours
            dt=1  # Default time step of 1 hour
        )
        if plot_model == True:
            plt.fill_between(
                persistence_mean.index,
                persistence_mean - persistence_std,
                persistence_mean + persistence_std,
                color='C2',
                alpha=0.2,
                label='Persistence Std Hourly'
            )
            plt.plot(deterministic.index, deterministic, label='Deterministic Forecast Hourly', color='red')
            plt.title('Forecast Comparison')
            plt.xlabel('Time')
            plt.ylabel(column)
            plt.legend()
            #plt.show()
            pass

        return_df = pd.DataFrame({
            'forecast': deterministic,
            'forecast_std': persistence_std,
        })

        # shift index to index[0] = start time
        t_diff = start - return_df.index[0]
        return_df.index = return_df.index + t_diff

        return return_df







def apply_kalman_filter(series: pd.Series, process_var=1e-5, meas_var=0.1) -> pd.Series:
    """
    Apply a basic 1D Kalman Filter to a pandas Series.

    Parameters:
        series (pd.Series): The input time series.
        process_var (float): Process variance (Q).
        meas_var (float): Measurement variance (R).

    Returns:
        pd.Series: Smoothed series.
    """
    kf = KalmanFilter(dim_x=2, dim_z=1)

    # State: [position, velocity]
    kf.x = np.array([[series.iloc[0]], [0.]])  # initial state
    kf.F = np.array([[1., 1.], [0., 1.]])  # linear state transition matrix
    kf.H = np.array([[1., 0.]])  # measurement function
    kf.P *= 1000.  # covariance matrix
    kf.R = meas_var  # measurement noise
    kf.Q = np.array([[0.25, 0.5],  # process noise
                     [0.5, 1.0]]) * process_var

    results = []
    for z in series:
        kf.predict()
        kf.update([z])
        results.append(kf.x[0, 0])  # position estimate

    return pd.Series(results, index=series.index, name="kalman")



def persistence_3_week(
        start: TimestampType,
        last_3w_ts: pd.DataFrame,
) -> (pd.Series, pd.Series):
    past_ts = last_3w_ts.copy()

    def timestamp_to_week(timestamp: pd.Timestamp) -> int:
        return timestamp.weekday() * 24 + timestamp.hour

    week_hours = 7 * 24
    start = last_3w_ts.index[0].floor("h")
    week_offset = timestamp_to_week(start)
    past_ts.index = pd.to_datetime(past_ts.index).tz_convert(None)
    past_ts.index = (past_ts.index.map(timestamp_to_week) - week_offset) % week_hours

    agg_df = past_ts.groupby(past_ts.index).agg(['mean', 'std'])

    # Align aggregated df to start time  till start + 1 week
    aligned_index = ((pd.Series(range(week_hours)) + week_offset) % week_hours)
    aligned_index.index = pd.date_range(
        start=start,# + pd.Timedelta(weeks=20),
        periods=week_hours,
        freq="h")

    persistence_mean = agg_df['mean']
    persistence_mean.index = aligned_index.index

    persistence_std = agg_df['std']
    persistence_std.index = aligned_index.index

    return persistence_mean, persistence_std


def deterministic_forecast_model(
        current,
        forecast_values:pd.Series,
        t_half,
        dt=1):
    """
    F = external forecast values
    R = model results
    R_k+1 = F_k+1 + kappa * (R_k - F_k)
    kappa = 2^dt/t_half
    """
    kappa = 2 ** -(dt / t_half)
    results = [current]
    for index in range(1, len(forecast_values)):
        results.append(forecast_values.iloc[index] + kappa * (results[index - 1] - forecast_values.iloc[index - 1]))
    return pd.Series(results, index=forecast_values.index, name=forecast_values.name)

