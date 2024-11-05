# -*- coding: utf-8 -*-
"""
loris.connectors.sql.table
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Sequence
from itertools import chain
from typing import Any, Optional

from sqlalchemy import text, select, delete
from sqlalchemy import Integer, Float, Boolean, String, TIMESTAMP
from sqlalchemy import Table as SATable
from sqlalchemy.schema import Column as SAColumn
from sqlalchemy.types import TypeEngine

import pandas as pd

from loris.connectors.sql import Column, Columns, Index
from loris.core import Configurations, Resources


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
            resources: Resources
    ) -> Table:
        index = Index.from_configs(configs.get_section("index", defaults={}), timezone=connector._timezone)

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
            'integer': Integer,
            'string': String,
            'float': Float,
            'boolean': Boolean,
            'timestamp': TIMESTAMP
        }
        return dtype_mapping.get(dtype.lower(), String)

    @staticmethod
    def convert_to_sqlalchemy_columns(column_definitions):
        sqlalchemy_columns = []
        for col in column_definitions:
            col_name = col.name
            col_type = col.type.upper()
            nullable = not ('NOT NULL' in col.flags)

            if col_type == 'TIMESTAMP':
                sqlalchemy_columns.append(SAColumn(col_name, TIMESTAMP, nullable=nullable))
            elif col_type == 'FLOAT':
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
            start: Optional[pd.Timestamp, dt.datetime] = None,
            end: Optional[pd.Timestamp, dt.datetime] = None,
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

    def select_first(self, resources: Resources) -> pd.DataFrame:
        table = self.get_sqlalchemy_table()
        query = select(table).order_by(table.c[self.index.names[0]].asc()).limit(1)
        result = self.connection.execute(query)
        data = pd.DataFrame(result.fetchall(), columns=result.keys())
        return self.index.process(resources, data)

    def select_last(self, resources: Resources) -> pd.DataFrame:
        table = self.get_sqlalchemy_table()
        query = select(table).order_by(table.c[self.index.names[0]].desc()).limit(1)
        result = self.connection.execute(query)
        data = pd.DataFrame(result.fetchall(), columns=result.keys())
        return self.index.process(resources, data)

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
        table = self.get_sqlalchemy_table()

        query = delete(table).where(
            (table.c[self.index.names[0]] >= start) & (table.c[self.index.names[0]] <= end)
        )

        self._logger.debug(query)
        self.connection.execute(query)
        self.connection.commit()

    # noinspection PyUnresolvedReferences
    def insert(self, resources: Resources, data: pd.DataFrame) -> None:
        def _extract(d: pd.DataFrame) -> Sequence[Any]:
            return d.apply(lambda r: self.index.extract(r) + self.columns.extract(r), axis="columns").values

        table = self.get_sqlalchemy_table()
        query = table.insert()

        self._logger.debug(query)

        params = list(chain.from_iterable(_extract(d) for d in self.index.prepare(resources, data)))

        for param in params:
            param_dict = {
                key.name if hasattr(key, "name") else str(key): value
                for key, value in zip(self.index.names + self.columns.names, param)
            }

            if "timestamp" in param_dict.keys():
                param_dict["timestamp"] = param_dict["timestamp"].strftime("%Y-%m-%d %H:%M:%S")

            self.connection.execute(query, param_dict)

        self.connection.commit()

    def _get_column_type(self, column_name: str) -> TypeEngine:
        table = self.get_sqlalchemy_table()
        if column_name in table.c:
            return type(table.c[column_name].type)
        else:
            raise ValueError(f"Column '{column_name}' does not exist in table '{table.name}'.")
