# -*- coding: utf-8 -*-
"""
loris.connectors.mysql.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any, Dict, Iterator, List, Optional

from mysql import connector

import pandas as pd
from loris.connectors import ConnectionException, Connector, ConnectorException, register_connector_type
from loris.connectors.mysql import MySqlColumn, MySqlTable
from loris.core import Configurations, Resources
from loris.util import resample


@register_connector_type
class MySqlConnector(Connector, Mapping[str, MySqlTable]):
    TYPE: str = "mysql"

    _connection = None
    _tables: Dict[str, MySqlTable] = {}

    _table_index_column: str = MySqlTable.DEFAULT_INDEX_COLUMN
    _table_index_type: str = MySqlTable.DEFAULT_INDEX_TYPE
    _table_data_column: str = MySqlTable.DEFAULT_DATA_COLUMN
    _table_data_type: str = MySqlTable.DEFAULT_DATA_TYPE
    _tables_create: bool = True

    user: str
    password: str
    database: str

    host: str = "127.0.0.1"
    port: int = 3306

    resolution: Optional[int] = None

    @property
    def connection(self):
        if self._connection is None or not self._connection.is_connected():
            raise ConnectionException("MySQL Connection not open", connector=self)
        return self._connection

    def __getitem__(self, table_name: str) -> MySqlTable:
        return self._tables[table_name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._tables)

    def __len__(self) -> int:
        return len(self._tables)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        if configs.has_section(MySqlTable.SECTION):
            table = configs.get_section(MySqlTable.SECTION)
            self._table_index_column = table.get("index_column", default=MySqlConnector._table_index_column)
            self._table_index_type = table.get("index_type", default=MySqlConnector._table_index_type)
            self._table_data_column = table.get("data_column", default=MySqlConnector._table_data_column)
            self._table_data_type = table.get("data_type", default=MySqlConnector._table_data_type)
            self._tables_create = table.get_bool("create", default=MySqlConnector._tables_create)

        # TODO: Validate if minutely default resolution is sufficient
        resolution = configs.get_int("resolution", default=MySqlConnector.resolution)
        if resolution is not None:
            resolution *= 60
        self.resolution = resolution

        self.host = configs.get("host", MySqlConnector.host)
        self.port = configs.get_int("port", MySqlConnector.port)

        self.user = configs.get("user")
        self.password = configs.get("password")

        self.database = configs.get("database")

    def connect(self, resources: Resources) -> None:
        self._logger.info(f"Connecting to MySQL database {self.database}@{self.host}:{self.port}")
        self._connection = connector.connect(
            host=self.host, port=self.port, user=self.user, passwd=self.password, database=self.database
        )
        self._tables = self._load_tables(resources)

    def disconnect(self) -> None:
        if self._connection is not None:
            self._connection.close()

    def _load_tables(self, resources: Resources) -> Dict[str, MySqlTable]:
        tables = {}
        table_schemas = self._get_table_schemas()
        for table_schema in table_schemas:
            table_columns = []
            for table_column in self._get_column_schemas(table_schema["table_name"]):
                table_columns.append(
                    MySqlColumn(
                        table_column["column_name"],
                        table_column["data_type"],
                        primary="PRI" in table_column["column_key"],
                        nullable="YES" == table_column["is_nullable"],
                    )
                )
            table_index = table_columns.pop(0)
            table = MySqlTable(
                self, table_schema["table_name"], table_index, table_columns, engine=table_schema["engine"]
            )
            tables[table.name] = table

        for table_name, table_resources in resources.groupby("table"):
            if table_name in tables:
                continue
            if self._tables_create:
                # noinspection PyTypeChecker
                table = self._create_table(table_name, table_resources)
                tables[table.name] = table
            else:
                raise ConnectorException(f"Unable to find configured table: {table_name}", connector=self)

        return tables

    def _create_table(self, name: str, resources: Resources):
        index = MySqlColumn(
            self._table_index_column,
            self._table_index_type,
            primary=True,
            nullable=False
        )
        columns = []
        for resource in resources:
            column_name = resource.column if "column" in resource else resource.id
            column_args = {}
            if "nullable" in resource:
                column_args["nullable"] = resource.nullable
            if "primary" in resource:
                column_args["primary"] = resource.primary
            if "length" in resource:
                column_args["type_length"] = resource.length

            column_type = resource.type if "type" in resource else self._table_data_type
            columns.append(MySqlColumn(column_name, column_type, **column_args))

        table = MySqlTable(self, name, index, columns)
        table.create()

        return table

    def _get_column_schemas(self, table: str, select=None) -> List[Dict[str, Any]]:
        if select is None:
            select = ["column_name", "is_nullable", "data_type", "column_key"]

        cursor = self.connection.cursor()
        cursor.execute(
            f"SELECT {','.join(f'`{c}`' for c in select)} "
            f"FROM information_schema.columns "
            f"WHERE `table_schema`='{self.database}' AND `table_name`='{table}'"
        )

        columns = []
        for table_params in cursor.fetchall():
            columns.append(dict(zip(select, table_params)))
        return columns

    def _get_table_schemas(self, select=None) -> List[Dict[str, Any]]:
        # select = ['table_name', 'table_rows', 'engine', 'create_time', 'update_time']
        if select is None:
            select = ["table_name", "engine"]

        cursor = self.connection.cursor()
        cursor.execute(
            f"SELECT {','.join(f'`{c}`' for c in select)} "
            f"FROM information_schema.tables WHERE `table_schema`='{self.database}'"
        )

        tables = []
        for table_params in cursor.fetchall():
            tables.append(dict(zip(select, table_params)))
        return tables

    def create(
        self,
        name: str,
        columns: Optional[List[MySqlColumn]] = None,
        engine: str = None,  # 'MyISAM'
    ) -> MySqlTable:
        table = MySqlTable(self, name, columns, engine)
        table.create()

        self._tables[table.name] = table
        return table

    def exists(
        self,
        resources: Optional[Resources] = None,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> bool:
        # TODO: Replace this placeholder more resource efficient
        if resources is None:
            resources = self.resources
        return not self.read(resources, start, end).empty

    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> pd.DataFrame:
        data = pd.DataFrame(columns=[r.uuid for r in resources])

        for table_name, table_resources in resources.groupby("table"):
            if table_name not in self._tables:
                raise ConnectorException(f"Table '{table_name}' not available", connector=self)

            table_columns = [r.column if "column" in r else r.id for r in table_resources]
            table = self.get(table_name)

            if start is None and end is None:
                table_data = table.select_last(table_columns)
            else:
                table_data = table.select(table_columns, start, end)
                if self.resolution is not None:
                    try:
                        table_data = resample(table_data, self.resolution)

                    except TypeError as e:
                        self._logger.exception(str(e))

            for table_resource in table_resources:
                table_resource_column = table_resource.id if "column" not in table_resource else table_resource.column
                data[table_resource.uuid] = table_data.loc[:, table_resource_column]
        return data

    def write(self, data: pd.DataFrame) -> None:
        for table_name, table_resources in self.resources.groupby("table"):
            if table_name not in self._tables:
                raise ConnectorException(f"Table '{table_name}' not available", connector=self)

            table_data = data.loc[:, [r.uuid for r in table_resources if r.uuid in data.columns]]
            table_columns = {r.uuid: r.column if "column" in r else r.id for r in table_resources}

            table = self.get(table_name)
            table.insert(table_data.rename(columns=table_columns))

    def is_connected(self) -> bool:
        if self._connection is not None:
            return self._connection.is_connected()
        return False
