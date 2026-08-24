# -*- coding: utf-8 -*-
"""
lories._core.typing
~~~~~~~~~~~~~~~~~~~


"""

import datetime as dt
from dateutil.relativedelta import relativedelta
from typing import TypeVar, Union

import pandas as pd
import pytz as tz

Timestamp = TypeVar("Timestamp", pd.Timestamp, dt.datetime)
Timezone = TypeVar("Timezone", tz.BaseTzInfo, dt.tzinfo)
Duration = Union[pd.Timedelta, relativedelta]

__all__ = [
    "Timestamp",
    "Timezone",
    "Duration",
]
