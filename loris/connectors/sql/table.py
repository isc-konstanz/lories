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

import pandas as pd
from sqlalchemy import MetaData, text
from sqlalchemy.ext.declarative import declarative_base

from loris.connectors.sql import Column, Columns, Index
from loris.core import Configurations, Resources

Base = declarative_base()
metadata = MetaData()


class Table(Sequence[Column]):
    SECTION = "tables"

    index: Index
    columns: Columns

    # noinspection PyProtectedMember
    @classmethod
    def from_configs(cls, connector, name: str, configs: Configurations, resources: Resources) -> Table:
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

    def create(self):
        columns = [*self.index, *self.columns]
        query = (
            f"CREATE TABLE IF NOT EXISTS `{self.name}` "
            f"({', '.join([str(c) for c in columns])}, PRIMARY KEY ({', '.join(self.index.names)}))"
        )
        if self.engine is not None:
            query += f" ENGINE={self.engine}"

        self._logger.debug(query)
        self.connection.execute(text(query))
        self.connection.commit()

        return self

    def exists(
        self,
        resources: Optional[Resources] = None,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> bool:
        # TODO: Replace this placeholder more resource efficient
        return not self.select(resources, start, end).empty

    def select(
            self,
            resources: Resources,
            start: pd.Timestamp | dt.datetime = None,
            end: pd.Timestamp | dt.datetime = None,
    ) -> pd.DataFrame:
        query = f"SELECT {self.index.names}, {self.columns.names} FROM `{self.name}`"
        query, params = self.index.where(query, start, end)
        query += f" {self.index.order_by('ASC')}"
        return self._select(resources, query, params)

    def select_first(self, resources: Resources) -> pd.DataFrame:
        query = text(f"SELECT {self.index.names}, {self.columns.names} "
                     f"FROM `{self.name}` {self.index.order_by('ASC')} LIMIT 1;")
        return self._select(resources, query)

    def select_last(self, resources: Resources) -> pd.DataFrame:
        query = text(f"SELECT {self.index.names}, {self.columns.names} "
                     f"FROM `{self.name}` {self.index.order_by('DESC')} LIMIT 1;")
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
        query = text(f"DELETE FROM `{self.name}`")
        query, params = self.index.where(query, start, end)

        self._logger.debug(query)
        self.connection.execute(text(query), params)
        self.connection.commit()

    # noinspection PyUnresolvedReferences
    def insert(self, resources: Resources, data: pd.DataFrame) -> None:
        def _extract(d: pd.DataFrame) -> Sequence[Any]:
            return d.apply(lambda r: self.index.extract(r) + self.columns.extract(r), axis="columns").values

        query = (
            f"INSERT INTO `{self.name}` ({self.index}, {self.columns}) "
            f"VALUES ({', '.join([':' + col for col in self.index.names + self.columns.names])}) "
            f"ON DUPLICATE KEY UPDATE {', '.join([f'`{col.name}`=VALUES(`{col.name}`)' for col in self.columns])}"
        )

        self._logger.debug(query)

        params = list(chain.from_iterable(_extract(d) for d in self.index.prepare(resources, data)))

        for param in params:
            param_dict = {key.name if hasattr(key, 'name') else str(key): value
                          for key, value in zip(self.index.names + self.columns.names, param)}
            self.connection.execute(text(query), param_dict)

        self.connection.commit()

    def _get_column_type(self, column: str) -> str:
        select = (
            "SELECT data_type FROM information_schema.COLUMNS "
            "WHERE table_schema = :database "
            "AND table_name = :table_name "
            "AND column_name = :column"
        )
        result = self.connection.execute(text(select), {
            'database': self.connection.engine.url.database,
            'table_name': self.name,
            'column': column
        })
        row = result.fetchone()

        if row:
            return row[0].upper()
        else:
            raise ValueError(f"Column '{column}' not found in table '{self.name}'.")
