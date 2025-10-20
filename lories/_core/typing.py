# -*- coding: utf-8 -*-
"""
lories._core.typing
~~~~~~~~~~~~~~~~~~~


"""

import datetime as dt
from typing import TypeVar

import pandas as pd
import pytz as tz

Timestamp = TypeVar("Timestamp", pd.Timestamp, dt.datetime)
Timezone = TypeVar("Timezone", tz.BaseTzInfo, dt.tzinfo)

__all__ = [
    "Timestamp",
    "Timezone",
]
