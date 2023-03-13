# -*- coding: utf-8 -*-
"""
    corsys.io.sql
    ~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

import logging
import pytz as tz
import datetime as dt
import pandas as pd

from . import Database
from ..tools import to_int, resample_data
from mysql import connector

logger = logging.getLogger(__name__)


class SqlDatabase(Database):

    def __init__(self, host="127.0.0.1", port=3306,
                 user='root', password='', database='emondata',
                 tables=None, interval=24,
                 **kwargs):

        super().__init__(**kwargs)

        self.interval = to_int(interval)
        self.tables = tables
        if self.tables is None:
            self.tables = {}

        self.connector = connector.connect(
            host=host,
            port=to_int(port),
            user=user,
            passwd=password,
            database=database
        )

    def exists(self, **kwargs):
        data = self.read(**kwargs)
        return not data.empty

    def read(self,
             start: pd.Timestamp | dt.datetime = None,
             end:   pd.Timestamp | dt.datetime = None,
             resolution: int = None,
             **kwargs):

        results = list()
        for column, table in self.tables.items():
            results.append(self._select(column, table, start, end))

        try:
            data = pd.concat(results, axis=1)
            data.index.name = 'time'

            if resolution is not None:
                data = resample_data(data, resolution)

            return data

        except TypeError as e:
            logger.exception(str(e))

        return pd.DataFrame()

    def _select(self,
                column: str,
                table: str,
                start: pd.Timestamp | dt.datetime = None,
                end:   pd.Timestamp | dt.datetime = None) -> pd.DataFrame:

        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
        if start is None:
            start = epoch

        cursor = self.connector.cursor()
        select = "SELECT time, data FROM {0} WHERE ".format(table)
        if end is None:
            select += "time >= %s ORDER BY time ASC"
            cursor.execute(select, ((start.astimezone(tz.UTC)-epoch).total_seconds(),))
        else:
            select += "time BETWEEN %s AND %s ORDER BY time ASC"
            cursor.execute(select,
                           ((start.astimezone(tz.UTC)-epoch).total_seconds(),
                            (end.astimezone(tz.UTC)-epoch).total_seconds()))

        times = []
        values = []
        for timestamp, value in cursor.fetchall():
            time = dt.datetime.fromtimestamp(timestamp, tz=self.timezone)
            times.append(time)
            values.append(value)

        result = pd.DataFrame(data=values, index=times, columns=[column])
        return result.tz_convert(self.timezone)

    def write(self, data: pd.DataFrame, **_):
        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)

        cursor = self.connector.cursor()
        for column in data.columns:
            insert = "INSERT INTO {0} (time,data) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE data=VALUES(data)"\
                .format(self.tables[column])

            values = []
            for index, value in data[column].items():
                time = (index.astimezone(tz.UTC)-epoch).total_seconds()
                values.append((time, value))

            cursor.executemany(insert, values)

        self.connector.commit()
