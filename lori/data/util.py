# -*- coding: utf-8 -*-
"""
lori.data.util
~~~~~~~~~~~~~~


"""

from __future__ import annotations

import hashlib
import re
from copy import copy, deepcopy
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import pytz as tz
from pandas.tseries.frequencies import to_offset
import re

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


# noinspection PyShadowingBuiltins
def hash_data(
    data: pd.DataFrame,
    method: Literal["MD5", "SHA1", "SHA256", "SHA512"] = "MD5",
    encoding: str = "UTF-8",
) -> str:
    index_column = data.index.name if data.index is not None else "index"
    data_columns = data.columns
    data = deepcopy(data)

    for column in data.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        data[column] = data[column].dt.tz_convert(tz.UTC).view(np.int64) // 10**9

    data[data.index.name] = data.index.tz_convert(tz.UTC).view(np.int64) // 10**9
    data = data[[index_column, *data_columns]]

    csv = data.to_csv(index=False, header=False, sep=",", decimal=".", float_format="%.10g")
    csv = ",".join(re.sub(r',,+', ',', line).strip(",") for line in csv.splitlines())
    return hash_value(csv, method, encoding)


def hash_value(
    value: str,
    method: Literal["MD5", "SHA1", "SHA256", "SHA512"],
    encoding: str = "UTF-8",
) -> str:
    method = method.lower()
    if method == "md5":
        return hashlib.md5(value.encode(encoding)).hexdigest()
    if method == "sha1":
        return hashlib.sha1(value.encode(encoding)).hexdigest()
    if method == "sha256":
        return hashlib.sha256(value.encode(encoding)).hexdigest()
    if method == "sha512":
        return hashlib.sha512(value.encode(encoding)).hexdigest()
    raise ValueError(f"Invalid checksum method '{method}'")


# noinspection PyUnresolvedReferences
def resample(
    data: pd.DataFrame | pd.Series,
    freq: str,
    func: Literal["sum", "mean", "min", "max", "last"],
    offset: Optional[pd.Timedelta] = None,
) -> pd.DataFrame | pd.Series:
    if func not in ["sum", "mean", "min", "max", "last"]:
        raise ValueError(f"Invalid resampling function '{func}'")
    freq = to_offset(freq)

    index = copy(data.index)
    index_freq = index.freq
    if index_freq is None and len(index) > 2:
        index_freq = to_offset(pd.infer_freq(index))
    if index_freq is None or index_freq < freq:
        resampled = data.resample(freq, closed="right", offset=offset)
        if func == "sum":
            data = resampled.sum()
        elif func == "mean":
            data = resampled.mean()
        elif func == "min":
            data = resampled.min()
        elif func == "max":
            data = resampled.max()
        elif func == "last":
            data = resampled.last()
        data.index += freq
    data.index.name = index.name

    return data


def scale_power(name: str, power: float) -> Tuple[str, float]:
    if power >= 1e7:
        power = round(power / 1e6, 2)
        name = name.replace("W", "MW")
    elif power >= 1e4:
        power = round(power / 1e3, 2)
        name = name.replace("W", "kW")
    else:
        power = round(power, 2)
    return name, power


def scale_energy(name: str, energy: float) -> Tuple[str, float]:
    if energy >= 1e7:
        energy = round(energy / 1e6, 2)
        name = name.replace("kWh", "GWh")
    elif energy >= 1e4:
        energy = round(energy / 1e3, 2)
        name = name.replace("kWh", "MWh")
    else:
        energy = round(energy, 2)
    return name, energy


def derive_by_hours(data: pd.Series) -> pd.Series:
    """
    Derive a data series by hours.

    Parameters
    ----------
    data : pandas.Series
        Series with the data to be derived

    Returns
    ----------
    fixed: pandas.Series
        Series with the derived data

    """
    delta_value = data.iloc[:].astype("float64").diff()

    delta_index = pd.Series(delta_value.index, index=delta_value.index)
    delta_index = (delta_index - delta_index.shift(1)) / np.timedelta64(1, "h")

    return pd.Series(delta_value / delta_index, index=data.index).dropna()
