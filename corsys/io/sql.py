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

from . import Database, DatabaseException
from ..tools import to_int, resample_data
from mysql import connector

logger = logging.getLogger(__name__)


class SqlDatabase(Database):

    def __init__(self, host="127.0.0.1", port=3306,
                 user='root', password='', database='emondata',
                 tables=None, interval=24,
                 **kwargs):
        super().__init__(**kwargs)
        self._connection = None

        self.interval = to_int(interval)
        self.tables = tables
        if self.tables is None:
            self.tables = {}

        self.host = host
        self.port = to_int(port)
        self.user = user
        self.password = password
        self.database = database

    def __open__(self, **_) -> None:
        self._connection = connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password,
            database=self.database
        )

    def __close__(self) -> None:
        self._connection.close()

    @property
    def connection(self):
        if self._connection is None:
            raise DatabaseException("SQL Connection not open")
        return self._connection

    def exists(self, **kwargs):
        # TODO: Replace this placeholder more resource efficient
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

        cursor = self.connection.cursor()
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

        cursor = self.connection.cursor()
        for column in data.columns:
            insert = "INSERT INTO {0} (time,data) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE data=VALUES(data)"\
                .format(self.tables[column])

            values = []
            for index, value in data[column].items():
                time = (index.astimezone(tz.UTC)-epoch).total_seconds()
                values.append((time, value))

            cursor.executemany(insert, values)

        self.connection.commit()
