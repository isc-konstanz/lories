# -*- coding: utf-8 -*-
"""
    corsys.io.sql
    ~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Optional, Any, Dict, List, Tuple

import logging
import pytz as tz
import datetime as dt
import pandas as pd

from . import Database, DatabaseException
from ..tools import to_int, resample
from mysql import connector

logger = logging.getLogger(__name__)


class SqlDatabase(Database):

    def __init__(self, host="127.0.0.1", port=3306,
                 user='root', password='', database='emondata',
                 tables=None, create_tables=False, **kwargs):
        super().__init__(**kwargs)
        self._connection = None

        self.tables = {}
        self._tables_create = create_tables
        self._table_columns = {t: c for c, t in tables.items()} if tables is not None else {}

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
        self._read_tables()

    def __close__(self) -> None:
        self._connection.close()

    @property
    def connection(self):
        if self._connection is None or not self._connection.is_connected():
            raise DatabaseException("SQL Connection not open")
        return self._connection

    def create(self, name, **kwargs):
        table = SqlTable(self._connection, name)
        table.create(**kwargs)

        self.tables[table.name] = table
        return table

    def exists(self, **kwargs):
        # TODO: Replace this placeholder more resource efficient
        data = self.read(**kwargs)
        return not data.empty

    def _get_table_schemas(self, columns=None) -> List[Dict[str, Any]]:
        # columns = ['table_name', 'table_rows', 'engine', 'create_time', 'update_time']
        if columns is None:
            columns = ['table_name', 'engine']
        select = f"SELECT {','.join(f'`{c}`' for c in columns)} " \
                 f"FROM information_schema.tables WHERE `table_schema`='{self.database}'"

        cursor = self.connection.cursor()
        cursor.execute(select)

        tables = []
        for table_params in cursor.fetchall():
            tables.append(dict(zip(columns, table_params)))
        return tables

    def _read_tables(self) -> Dict[str, SqlTable]:
        # Clear tables first
        self.tables = {}
        for table_schema in self._get_table_schemas():
            table = SqlTable(self._connection, **table_schema)
            if len(self._table_columns) == 0 or table.name in self._table_columns:
                self.tables[table.name] = table

        missing_tables = [t for t in self._table_columns.keys() if t not in self.tables]
        if len(missing_tables) > 0:
            if self._tables_create:
                for table_name in missing_tables:
                    table = SqlTable(self._connection, table_name)

                    self.tables[table.name] = table.create()
            else:
                raise DatabaseException(f"Unable to find configured table{'s' if len(missing_tables) > 0 else ''}: "
                                        ','.join(missing_tables))

        return self.tables

    def read(self,
             start: pd.Timestamp | dt.datetime = None,
             end:   pd.Timestamp | dt.datetime = None,
             resolution: int = None,
             columns: List[str] = None,
             **kwargs):

        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
        if start is None:
            start = epoch

        data = []
        for table in self.tables.values():
            data.append(table.get(start, end).tz_convert(self.timezone))
        try:
            data = pd.concat(data, axis='index')
            data = data.rename(columns=self._table_columns)
            data.index.name = 'time'

            if resolution is not None:
                data = resample(data, resolution)

            return data

        except TypeError as e:
            logger.exception(str(e))

        return pd.DataFrame()

    def write(self, data: pd.DataFrame, **_):
        for table in self.tables.values():
            if table.name in data.columns:
                table.write(data[[self._table_columns[table.name]]])


class SqlTable:

    INDEX = 'time'
    COLUMN = 'data'

    def __init__(self, connection, table_name, engine='MyISAM', **_):
        self._connection = connection

        self.name = table_name
        self.engine = engine

    @property
    def connection(self):
        if self._connection is None or not self._connection.is_connected():
            raise DatabaseException("SQL Connection not open")
        return self._connection

    def create(self, data_type='FLOAT'):
        create = f"CREATE TABLE IF NOT EXISTS {self.name} " \
                 f"({self.INDEX} INT UNSIGNED NOT NULL, {self.COLUMN} {data_type}, UNIQUE ({self.INDEX})) " \
                 f"ENGINE={self.engine}"

        with self.connection.cursor() as cursor:
            cursor.execute(create)

            self.connection.commit()
        return self

    def get_type(self, column: str = COLUMN) -> str:
        with self.connection.cursor(buffered=True) as cursor:
            select = f"SELECT data_type FROM information_schema.COLUMNS " \
                     f"WHERE table_schema = '{self.connection.database}' " \
                     f"AND table_name = '{self.name}' " \
                     f"AND column_name = '{column}'"
            cursor.execute(select)
            return cursor.fetchone()[0].upper()

    def get_first(self) -> Tuple[Optional[pd.Timestamp], Optional[Any]]:
        with self.connection.cursor(buffered=True) as cursor:
            select = f"SELECT time, data FROM {self.name} ORDER BY time ASC LIMIT 1;"
            cursor.execute(select)
            if cursor.rowcount > 0:
                timestamp, value = cursor.fetchone()
                time = pd.Timestamp.fromtimestamp(timestamp).tz_localize(tz.UTC)
                return time, value
        return None, None

    def get_last(self) -> Tuple[Optional[pd.Timestamp], Optional[Any]]:
        with self.connection.cursor(buffered=True) as cursor:
            select = f"SELECT time, data FROM {self.name} ORDER BY time DESC LIMIT 1;"
            cursor.execute(select)
            if cursor.rowcount > 0:
                timestamp, value = cursor.fetchone()
                time = pd.Timestamp.fromtimestamp(timestamp).tz_localize(tz.UTC)
                return time, value
        return None, None

    def get(self,
            start: pd.Timestamp | dt.datetime = None,
            end:   pd.Timestamp | dt.datetime = None) -> pd.Series:

        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)

        with self.connection.cursor() as cursor:
            select = f"SELECT time, data FROM {self.name}"
            if start is not None and end is not None:
                select += " WHERE time BETWEEN %s AND %s ORDER BY time ASC"
                cursor.execute(select,
                               ((start.astimezone(tz.UTC)-epoch).total_seconds(),
                                (end.astimezone(tz.UTC)-epoch).total_seconds()))
            elif start is not None:
                select += " WHERE time >= %s ORDER BY time ASC"
                cursor.execute(select, ((start.astimezone(tz.UTC)-epoch).total_seconds(),))

            elif end is not None:
                select += " WHERE time <= %s ORDER BY time ASC"
                cursor.execute(select, ((end.astimezone(tz.UTC)-epoch).total_seconds(),))

            times = []
            values = []
            for timestamp, value in cursor.fetchall():
                time = dt.datetime.fromtimestamp(timestamp, tz.UTC)
                times.append(time)
                values.append(value)

            return pd.Series(index=times, data=values, name=self.name)

    def delete(self,
               start: pd.Timestamp | dt.datetime,
               end:   pd.Timestamp | dt.datetime) -> None:
        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)

        if start is None or end is None:
            return
        with self.connection.cursor() as cursor:
            select = f"DELETE FROM {self.name}" \
                     " WHERE time BETWEEN %s AND %s"
            cursor.execute(select,
                           ((start.astimezone(tz.UTC)-epoch).total_seconds(),
                            (end.astimezone(tz.UTC)-epoch).total_seconds()))

    # noinspection PyUnresolvedReferences
    def write(self, data: pd.DataFrame | pd.Series, **_):
        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)

        # Make sure to iterate over a Series, not a DataFrame
        if isinstance(data, pd.DataFrame):
            data = data[self.name]

        with self.connection.cursor() as cursor:
            insert = f"INSERT INTO {self.name} (time,data) VALUES ('%s', '%s') " \
                     "ON DUPLICATE KEY UPDATE data=VALUES(data)"

            values = []
            for index, value in data.items():
                time = (index.astimezone(tz.UTC)-epoch).total_seconds()
                values.append((time, value))

            cursor.executemany(insert, values)

            self.connection.commit()
