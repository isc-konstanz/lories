# -*- coding: utf-8 -*-
"""
lori.core.typing
~~~~~~~~~~~~~~~~


"""

import datetime as dt
from typing import Iterable, TypeVar

import pandas as pd
import pytz as tz
from lori.data import Channel, Channels

TimestampType = TypeVar("TimestampType", pd.Timestamp, dt.datetime)
TimezoneType = TypeVar("TimezoneType", tz.BaseTzInfo, dt.tzinfo)
ChannelsType = TypeVar("ChannelsType", Channel, Channels, Iterable[Channel], Iterable[str], str)
