# -*- coding: utf-8 -*-
"""
lori.connectors.sql.table
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
from collections.abc import Sequence
from itertools import chain
from typing import Any, Optional

from sqlalchemy import TIMESTAMP, Boolean, Float, Integer, String, delete, select, text
from sqlalchemy import Table as SATable
from sqlalchemy.schema import Column as SAColumn
from sqlalchemy.types import TypeEngine

import numpy as np
import pandas as pd
from lori.connectors import ConnectorException
from lori.connectors.sql import Column, Columns, Index
from lori.core import Configurations, Resources


class Table(Sequence[Column]):
    SECTION = "tables"

    index: Index
    columns: Columns

    # noinspection PyProtectedMember
    @classmethod
    def from_configs(
        cls,
        connector,
        engine,
        metadata,
        schema,
        name: str,
        configs: Configurations,
        resources: Resources,
    ) -> Table:
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

        return Table(connector, engine, metadata, schema, name, index, columns)

    def __init__(
        self,
        connector,
        engine,
        metadata,
        schema,
        name: str,
        index: Optional[Index] = None,
        columns: Optional[Columns] = None,
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
        self.metadata = metadata
        self.schema = schema
        self._logger = logging.getLogger(__name__)

    def __getitem__(self, index: int) -> Column:
        columns = [*self.index, *self.columns]
        return columns[index]

    def __len__(self):
        return len(self.index) + len(self.columns)

    @property
    def connection(self):
        return self._connector.connection

    @staticmethod
    def get_sacolumn_type(dtype: str) -> TypeEngine:
        dtype_mapping = {
            "integer": Integer,
            "string": String,
            "float": Float,
            "boolean": Boolean,
            "timestamp": TIMESTAMP,
        }
        return dtype_mapping.get(dtype.lower(), String)

    @staticmethod
    def convert_to_sqlalchemy_columns(column_definitions):
        sqlalchemy_columns = []
        for col in column_definitions:
            col_name = col.name
            col_type = col.type.upper()
            nullable = "NOT NULL" not in col.flags

            if col_type == "TIMESTAMP":
                sqlalchemy_columns.append(SAColumn(col_name, TIMESTAMP, nullable=nullable))
            elif col_type == "FLOAT":
                sqlalchemy_columns.append(SAColumn(col_name, Float, nullable=nullable))
            else:
                raise ValueError(f"Unsupported column type: {col_type}")
        return sqlalchemy_columns

    def get_sqlalchemy_table(self) -> SATable:
        return SATable(self.name, self.metadata, autoload_with=self.engine, schema=self.schema)

    def create(self):
        columns = self.convert_to_sqlalchemy_columns([*self.index, *self.columns])
        SATable(self.name, self.metadata, *columns)
        self.metadata.create_all(self.engine)
        return self

    def exists(
        self,
        resources: Optional[Resources] = None,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> bool:
        query = self.select(resources, start, end)
        # TODO: Replace this placeholder more resource efficient
        return not query.empty

    def select(
        self,
        resources: Resources,
        start: pd.Timestamp | dt.datetime = None,
        end: pd.Timestamp | dt.datetime = None,
    ) -> pd.DataFrame:
        # TODO: Implement querying only subset of resources
        table = self.get_sqlalchemy_table()
        query = select(table)

        if start:
            query = query.where(table.c[self.index.names[0]] >= start)
        if end:
            query = query.where(table.c[self.index.names[0]] <= end)

        query = query.order_by(table.c[self.index.names[0]].asc())
        result = self.connection.execute(query)
        data = pd.DataFrame(result.fetchall(), columns=result.keys())
        return self.index.process(resources, data)

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
                    f"Unable to generate consistent hashes for table '{self.name}' "
                    f"with DATETIME column: {column.name}"
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
        # TODO: Implement querying only subset of resources
        table = self.get_sqlalchemy_table()
        query = select(table).order_by(table.c[self.index.names[0]].asc()).limit(1)
        result = self.connection.execute(query)
        data = pd.DataFrame(result.fetchall(), columns=result.keys())
        return self.index.process(resources, data)

    def select_last(self, resources: Resources) -> pd.DataFrame:
        # TODO: Implement querying only subset of resources
        table = self.get_sqlalchemy_table()
        query = select(table).order_by(table.c[self.index.names[0]].desc()).limit(1)
        result = self.connection.execute(query)
        data = pd.DataFrame(result.fetchall(), columns=result.keys())
        return self.index.process(resources, data)

    def delete(self, start: pd.Timestamp | dt.datetime, end: pd.Timestamp | dt.datetime) -> None:
        table = self.get_sqlalchemy_table()
        query = delete(table).where((table.c[self.index.names[0]] >= start) & (table.c[self.index.names[0]] <= end))

        self._logger.debug(query)
        self.connection.execute(query)
        self.connection.commit()

    # noinspection PyUnresolvedReferences
    def insert(self, resources: Resources, data: pd.DataFrame) -> None:
        resources = resources.filter(lambda r: r.id in data.columns)
        resource_columns = self.columns.get(resources)
        columns = Columns(*self.index, *resource_columns)

        table = self.get_sqlalchemy_table()
        query = table.insert()

        # TODO: Are duplicate indexes addressed in the table.insert() query?
        # if len(column_names) > 0:
        #     query += f" ON DUPLICATE KEY UPDATE {', '.join([f'{c}=VALUES({c})' for c in column_names])}"
        # query = self.is_postgresql(query)

        self._logger.debug(query)

        def _extract(d: pd.DataFrame) -> Sequence[Any]:
            return d.apply(lambda r: columns.extract(r), axis="columns").values

        data.replace(np.nan, None, inplace=True)
        for params in chain.from_iterable(_extract(d) for d in self.index.prepare(resources, data)):
            self.connection.execute(query, params)
        self.connection.commit()

    def _get_column_type(self, column_name: str) -> TypeEngine:
        table = self.get_sqlalchemy_table()
        if column_name in table.c:
            return type(table.c[column_name].type)
        else:
            raise ValueError(f"Column '{column_name}' does not exist in table '{table.name}'.")
