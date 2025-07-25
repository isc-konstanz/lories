# -*- coding: utf-8 -*-
"""
lori.data.predictors.util
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import numpy as np
import pandas as pd

from lori.util import to_timedelta


def prediction_correction(
    value: float,
    prediction: pd.Series,
    horizon: str = "12h",
) -> pd.Series:
    """
    F = prediction
    R = results
    R_k+1 = F_k+1 + kappa * (R_k - F_k)
    kappa = 2^dt/t_half
    """
    index = prediction.index
    values = prediction.values
    seconds = index.to_series().diff().dt.total_seconds().bfill().values
    kappas = 2 ** - (seconds / to_timedelta(horizon).total_seconds())

    correction = np.empty_like(values, dtype=float)
    correction[0] = value

    for i in range(1, len(values)):
        correction[i] = values[i] + kappas[i] * (correction[i - 1] - values[i - 1])

    return pd.Series(correction, index=index, name=prediction.name)
