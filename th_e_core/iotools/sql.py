# -*- coding: utf-8 -*-
"""
    th-e-core.iotools.sql
    ~~~~~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

import pytz as tz
import datetime as dt
import pandas as pd

from th_e_core.iotools import Database
from th_e_core.tools import _int
from mysql import connector


class SqlDatabase(Database):

    def __init__(self, host="127.0.0.1", port=3306,
                 user='root', password='', database='emonpv',
                 tables=None, interval=24,
                 **kwargs):

        super().__init__(**kwargs)

        self.interval = _int(interval)
        self.tables = tables
        if self.tables is None:
            self.tables = {}

        self.connector = connector.connect(
            host=host,
            port=_int(port),
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

        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
        data = pd.DataFrame()

        for column, table in self.tables.items():
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
                time = tz.utc.localize(dt.datetime.fromtimestamp(timestamp))
                times.append(time)
                values.append(value)

            result = pd.DataFrame(data=values, index=times, columns=[column])
            data = pd.concat([data, result], axis=1)

        if resolution is not None and resolution > 900:
            offset = (start - start.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds() % resolution
            data = data.resample(str(int(resolution))+'s', base=offset).sum()

        return data

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
