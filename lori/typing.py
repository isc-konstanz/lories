# -*- coding: utf-8 -*-
"""
lori.core.typing
~~~~~~~~~~~~~~~~


"""

import datetime as dt
from typing import TypeVar

import pandas as pd
import pytz as tz

TimestampType = TypeVar("TimestampType", pd.Timestamp, dt.datetime)
TimezoneType = TypeVar("TimezoneType", tz.BaseTzInfo, dt.tzinfo)
