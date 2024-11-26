# -*- coding: utf-8 -*-
"""
lori.connectors.sql.table
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
import re
from collections.abc import Sequence
from itertools import chain
from typing import Any, Optional

from sqlalchemy import MetaData, text
from sqlalchemy.ext.declarative import declarative_base

import numpy as np
import pandas as pd
from lori.connectors import ConnectorException
from lori.connectors.sql import Column, Columns, Index
from lori.core import Configurations, Resources

Base = declarative_base()
metadata = MetaData()


class Table(Sequence[Column]):
    SECTION = "tables"

    index: Index
    columns: Columns

    # noinspection PyProtectedMember
    @classmethod
    def from_configs(cls, connector, name: str, configs: Configurations, resources: Resources) -> Table:
        index = Index.from_configs(configs.get_section("index", defaults={}), timezone=connector.timezone)

        columns_configs = configs.get_section("columns", defaults={})
        columns_type = columns_configs.get("type", default=Column.DEFAULT_TYPE)
        columns = Columns()

        for column_name in columns_configs.sections:
            column = columns_configs[column_name]
            column_type = column.pop("type", columns_type)
            if column.get_bool("primary", default=False) or "attribute" in column:
                del column["primary"]
                index.add(column_name, column_type, **column)
            else:
                columns.add(column_name, column_type, **column)

        for resource in resources:
            column_name = resource.column if "column" in resource else resource.key
            column_args = {}
            if "length" in resource:
                column_args["length"] = resource.length
            if "primary" in resource and resource.primary:
                index.add(column_name, resource.type, **column_args)
            else:
                if "nullable" in resource:
                    column_args["nullable"] = resource.nullable

                column_type = resource.type if "type" in resource else columns_type
                columns.add(column_name, column_type, **column_args)

        return Table(connector, name, index, columns)

    def __init__(
        self,
        connector,
        name: str,
        index: Optional[Index] = None,
        columns: Optional[Columns] = None,
        engine: str = None,  # 'MyISAM'
    ):
        self._connector = connector

        self.name = name

        if index is None:
            index = Index.from_defaults()
        self.index = index
        if columns is None:
            columns = Columns.from_defaults()
        self.columns = columns

        self.engine = engine

        self._logger = logging.getLogger(__name__)

    def __getitem__(self, index: int) -> Column:
        columns = [*self.index, *self.columns]
        return columns[index]

    def __len__(self):
        return len(self.index) + len(self.columns)

    @property
    def connection(self):
        return self._connector.connection

    def is_postgresql(self, query: str) -> str:
        if self._connector.dialect == "postgres":
            # Replace backticks with double quotes
            query = query.replace("`", '"')
            # Replace MySQL's ON DUPLICATE KEY UPDATE with PostgreSQL's ON CONFLICT
            query = re.sub(
                r"ON DUPLICATE KEY UPDATE.*",
                f"ON CONFLICT ({', '.join(self.index.names)}) DO UPDATE SET " ', '.join(
                    [f'"{col.name}"=EXCLUDED."{col.name}"' for col in self.columns]
                ),
                query,
            )
        return query

    def create(self):
        columns = Columns(*self.index, *self.columns)
        query = (
            f"CREATE TABLE IF NOT EXISTS `{self.name}` "
            f"({', '.join([f'`{c}`' for c in columns])}, PRIMARY KEY ({', '.join(self.index.names)}))"
        )
        if self.engine is not None:
            query += f" ENGINE={self.engine}"

        query = self.is_postgresql(query)

        self._logger.debug(query)
        self.connection.execute(text(query))
        self.connection.commit()

        return self

    def exists(
        self,
        resources: Optional[Resources] = None,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> bool:
        # TODO: Replace this placeholder more resource efficient
        return not self.select(resources, start, end).empty

    def select(
        self,
        resources: Resources,
        start: pd.Timestamp | dt.datetime = None,
        end: pd.Timestamp | dt.datetime = None,
    ) -> pd.DataFrame:
        columns = Columns(*self.index, *self.columns.get(resources))
        query = f"SELECT {', '.join([f'`{c.name}`' for c in columns])} FROM `{self.name}`"
        query, params = self.index.where(query, start, end)
        query += f" {self.index.order_by('ASC')}"
        query = self.is_postgresql(query)
        return self._select(resources, query, params)

    # noinspection PyProtectedMember
    def select_hash(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
        method: str = "MD5",
        encoding: str = "UTF-8",
    ) -> Optional[str]:
        if method.lower() not in ["md5"]:
            # TODO: Implement further checksum methods
            raise ValueError(f"Invalid checksum method '{method}'")

        def _prepare(column: Column) -> str:
            if column.type == "DATETIME":
                # TODO: Verify if there is a more generic way to implement time
                raise ConnectorException(
                    self._connector,
                    f"Unable to generate consistent hashes for table '{self.name}' "
                    f"with DATETIME column: {column.name}",
                )
            if column.type == "TIMESTAMP":
                return f"UNIX_TIMESTAMP(`{column.name}`)"
            else:
                return f"`{column.name}`"

        columns = self.columns.get(resources)
        column_names = [_prepare(c) for c in columns]
        index_names = [_prepare(c) for c in self.index]
        query = (
            f"SELECT {method.upper()}(GROUP_CONCAT(CONCAT_WS(',', {', '.join(index_names + column_names)})"
            f" {self.index.order_by('ASC')})) as `hash`,"
            f" 1 as `in_range` FROM `{self.name}`"  # Maybe group by NULL here?
        )

        query, params = self.index.where(query, start, end)
        if len(columns) > 0:
            # Make sure to only query valid values
            query += f" AND CONCAT({','.join([f'`{c.name}`' for c in columns])}) IS NOT NULL"

        query += " GROUP BY `in_range`"
        query = self.is_postgresql(query)

        result = self.connection.execute(text(query), params)
        if result.rowcount > 0:
            hashes = [r[0] for r in result.fetchall()]
            if len(hashes) == 1:
                return hashes[0]
            return hashlib.md5(",".join(hashes).encode(encoding)).hexdigest()
        return None

    def select_first(self, resources: Resources) -> pd.DataFrame:
        columns = Columns(*self.index, *self.columns.get(resources))
        query = f"SELECT {', '.join([f'`{c.name}`' for c in columns])} FROM `{self.name}`"

        # Make sure to only query valid values
        not_null = [f"`{c.name}` IS NOT NULL" for c in self.columns if c in columns]
        if len(not_null) > 0:
            query += f" WHERE {' AND '.join(not_null)}"

        query += f" {self.index.order_by('ASC')} LIMIT 1"
        query = self.is_postgresql(query)
        return self._select(resources, query)

    def select_last(self, resources: Resources) -> pd.DataFrame:
        columns = Columns(*self.index, *self.columns.get(resources))
        query = f"SELECT {', '.join([f'`{c.name}`' for c in columns])} FROM `{self.name}`"

        # Make sure to only query valid values
        not_null = [f"`{c.name}` IS NOT NULL" for c in self.columns if c in columns]
        if len(not_null) > 0:
            query += f" WHERE {' AND '.join(not_null)}"

        query += f" {self.index.order_by('DESC')} LIMIT 1"
        query = self.is_postgresql(query)
        return self._select(resources, query)

    def _select(
        self,
        resources: Resources,
        query: str,
        parameters: Sequence[Any] = (),
    ) -> pd.DataFrame:
        self._logger.debug(query)
        result = self.connection.execute(text(query), parameters)

        if result.rowcount > 0:
            data = pd.DataFrame(result.fetchall(), columns=result.keys())
        else:
            data = pd.DataFrame()

        return self.index.process(resources, data)

    def delete(self, start: pd.Timestamp | dt.datetime, end: pd.Timestamp | dt.datetime) -> None:
        query = f"DELETE FROM `{self.name}`"
        query = self.is_postgresql(query)
        query, params = self.index.where(query, start, end)

        self._logger.debug(query)
        self.connection.execute(text(query), params)
        self.connection.commit()

    # noinspection PyUnresolvedReferences
    def insert(self, resources: Resources, data: pd.DataFrame) -> None:
        resources = resources.filter(lambda r: r.id in data.columns)
        resource_columns = self.columns.get(resources)
        columns = Columns(*self.index, *resource_columns)
        column_names = [f"`{c.name}`" for c in resource_columns]
        index_names = [f"`{c.name}`" for c in self.index]

        query = (
            f"INSERT INTO `{self.name}` ({', '.join([c for c in index_names + column_names])}) "
            f"VALUES ({', '.join([c.placeholder for c in columns])})"
        )

        if len(column_names) > 0:
            query += f" ON DUPLICATE KEY UPDATE {', '.join([f'{c}=VALUES({c})' for c in column_names])}"
        query = self.is_postgresql(query)

        self._logger.debug(query)

        def _extract(d: pd.DataFrame) -> Sequence[Any]:
            return d.apply(lambda r: columns.extract(r), axis="columns").values

        data.replace(np.nan, None, inplace=True)
        for params in chain.from_iterable(_extract(d) for d in self.index.prepare(resources, data)):
            self.connection.execute(text(query), params)
        self.connection.commit()

    def _get_column_type(self, column: str) -> str:
        query = (
            "SELECT `data_type` FROM information_schema.COLUMNS "
            "WHERE `table_schema` = :database "
            "AND `table_name` = :table_name "
            "AND `column_name` = :column"
        )
        query = self.is_postgresql(query)
        params = {
            "database": self.connection.engine.url.database,
            "table_name": self.name,
            "column": column,
        }
        result = self.connection.execute(text(query), params)
        row = result.fetchone()
        if row:
            return row[0].upper()
        else:
            raise ValueError(f"Column '{column}' not found in table '{self.name}'.")
