# -*- coding: utf-8 -*-
"""
loris.connectors.mysql.table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import logging
from typing import List, Optional

import pandas as pd
from loris.connectors.mysql import MySqlColumn


class MySqlTable:
    SECTION = "table"

    DEFAULT_INDEX_COLUMN = "timestamp"
    DEFAULT_INDEX_TYPE = "TIMESTAMP"
    DEFAULT_INDEX = MySqlColumn(DEFAULT_INDEX_COLUMN, DEFAULT_INDEX_TYPE, nullable=False, primary=True)

    DEFAULT_DATA_COLUMN = "data"
    DEFAULT_DATA_TYPE = "FLOAT"
    DEFAULT_COLUMNS = [MySqlColumn(DEFAULT_DATA_COLUMN, DEFAULT_DATA_TYPE)]

    index: MySqlColumn

    columns: List[MySqlColumn]

    def __init__(
        self,
        connector,
        name: str,
        index: Optional[MySqlColumn] = None,
        columns: Optional[List[MySqlColumn]] = None,
        engine: str = None,  # 'MyISAM'
    ):
        self._connector = connector

        self.name = name

        if index is None:
            index = MySqlTable.DEFAULT_INDEX
        self.index = index
        if columns is None:
            columns = MySqlTable.DEFAULT_COLUMNS
        self.columns = columns

        self.engine = engine

        self._logger = logging.getLogger(__name__)

    @property
    def connection(self):
        return self._connector.connection

    def create(self):
        columns = [self.index] + self.columns
        primary = [c.name for c in columns if c.primary]

        query = (
            f"CREATE TABLE IF NOT EXISTS {self.name} "
            f"({', '.join([str(c) for c in columns])}, PRIMARY KEY ({', '.join(primary)}))"
        )

        if self.engine is not None:
            query += f" ENGINE={self.engine}"

        with self.connection.cursor() as cursor:
            self._logger.debug(query)
            cursor.execute(query)

            self.connection.commit()
        return self

    def exists(
        self,
        columns: Optional[List[str]] = None,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> bool:
        if columns is None:
            columns = [c.name for c in self.columns]

        # TODO: Replace this placeholder more resource efficient
        return not self.select(columns, start, end).empty

    def select(
        self,
        columns: Optional[List[str]] = None,
        start: pd.Timestamp | dt.datetime = None,
        end: pd.Timestamp | dt.datetime = None,
    ) -> pd.DataFrame:
        if columns is None:
            columns = [c.name for c in self.columns]
        query = f"SELECT {', '.join([self.index.name] + columns)} FROM {self.name}"
        where = []
        if start is not None:
            where.append(f"{self.index.name} >= {self.index.encode(start)}")
        if end is not None:
            where.append(f"{self.index.name} <= {self.index.encode(end)}")
        if len(where) > 0:
            query += " WHERE" + " AND".join(where)
        query += f" ORDER BY {self.index.name} ASC"
        return self._select(columns, query)

    def select_first(self, columns: Optional[List[str]] = None) -> pd.DataFrame:
        if columns is None:
            columns = [c.name for c in self.columns]
        query = f"SELECT {', '.join([self.index.name] + columns)} FROM {self.name} ORDER BY timestamp ASC LIMIT 1;"
        return self._select(columns, query)

    def select_last(self, columns: Optional[List[str]] = None) -> pd.DataFrame:
        if columns is None:
            columns = [c.name for c in self.columns]
        query = f"SELECT {', '.join([self.index.name] + columns)} FROM {self.name} ORDER BY timestamp DESC LIMIT 1;"
        return self._select(columns, query)

    def _select(self, column_names: List[str], query: str) -> pd.DataFrame:
        with self.connection.cursor(buffered=True) as cursor:
            self._logger.debug(query)
            if cursor.rowcount > 0:
                cursor.execute(query)
                columns = sorted([c for c in self.columns if c.name in column_names], key=lambda c: c.name)

                timestamps = []
                data = []
                for row in cursor.fetchall():
                    timestamps.append(self.index.decode(row[self.index.name]))
                    data.append([c.decode(row[c.name]) for c in columns])

                data = pd.DataFrame(columns=column_names, data=data, index=timestamps)
            else:
                data = pd.DataFrame(columns=column_names)

            data.index.name = self.index.name
        return data

    def delete(self, start: pd.Timestamp | dt.datetime, end: pd.Timestamp | dt.datetime) -> None:
        if start is None or end is None:
            return

        # TODO: Implement deleting only from specific columns ?
        query = f"DELETE FROM {self.name}"
        where = []
        if start is not None:
            where.append(f"{self.index.name} >= {self.index.encode(start)}")
        if end is not None:
            where.append(f"{self.index.name} <= {self.index.encode(end)}")
        query += " WHERE" + " AND".join(where)

        with self.connection.cursor() as cursor:
            self._logger.debug(query)
            cursor.execute(query)

    # noinspection PyUnresolvedReferences
    def insert(self, data: pd.DataFrame) -> None:
        columns = [c for c in self.columns if c.name in data.columns]

        query = f"INSERT INTO {self.name} (`{self.index.name}`, " + ", ".join([f"`{c.name}`" for c in columns]) + ")"
        query += " VALUES (%s, " + ", ".join(["%s"] * (len(columns))) + ")"
        query += " ON DUPLICATE KEY UPDATE " + ", ".join(
            [f"`{c.name}`=VALUES(`{c.name}`)" for c in columns if not c.primary]
        )
        with self.connection.cursor() as cursor:
            self._logger.debug(query)
            params = []
            for timestamp, values in data.iterrows():
                params.append((self.index.encode(timestamp), *[c.encode(values[c.name]) for c in columns]))

            cursor.executemany(query, params)

            self.connection.commit()

    def _get_column_type(self, column: str) -> str:
        with self.connection.cursor(buffered=True) as cursor:
            select = (
                f"SELECT data_type FROM information_schema.COLUMNS "
                f"WHERE table_schema = '{self.connection.database}' "
                f"AND table_name = '{self.name}' "
                f"AND column_name = '{column}'"
            )
            cursor.execute(select)
            return cursor.fetchone()[0].upper()
