# -*- coding: utf-8 -*-
"""
    corsys.io.oem
    ~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

import logging
import pytz as tz
import datetime as dt
import pandas as pd

from . import Database
from ..tools import to_int, resample
from emonpy import Emoncms

logger = logging.getLogger(__name__)


class EmonDatabase(Database):

    def __init__(self,
                 enabled: str = 'true',
                 timezone: str | tz.BaseTzInfo = tz.UTC,
                 feeds: dict = None,
                 **kwargs):
        super().__init__(enabled=enabled,
                         timezone=timezone)

        self.feeds = feeds
        if self.feeds is None:
            self.feeds = {}

        self.connector = Emoncms(**kwargs)

    def exists(self,
               start: pd.Timestamp | dt.datetime = None,
               end:   pd.Timestamp | dt.datetime = None,
               **_) -> bool:
        return self.connector.contains(start, end)

    # noinspection PyShadowingNames
    def read(self,
             start: pd.Timestamp | dt.datetime = None,
             end:   pd.Timestamp | dt.datetime = None,
             resolution: int = None,
             **kwargs):

        results = list()
        for feed, id in self.feeds.items():
            results.append(self.connector.feed(id, name=feed).data(start, end))

        try:
            data = pd.concat(results, axis=1)
            data.index.name = 'time'

            if resolution is not None:
                data = resample(data, resolution)

            return data

        except TypeError as e:
            logger.exception(str(e))

        return pd.DataFrame()

    def write(self, data: pd.DataFrame, **_):
        raise NotImplementedError()
