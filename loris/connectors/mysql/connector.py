# -*- coding: utf-8 -*-
"""
loris.connectors.mysql.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any, Dict, Iterator, Optional

from mysql import connector
from mysql.connector.errors import DatabaseError

import pandas as pd
import pytz as tz
from loris.connectors import ConnectionException, Connector, ConnectorException, register_connector_type
from loris.connectors.mysql import MySqlTable
from loris.core import Configurations, Resources


@register_connector_type
class MySqlConnector(Connector, Mapping[str, MySqlTable]):
    TYPE: str = "mysql"

    _connection = None
    _tables: Dict[str, MySqlTable] = {}

    _timezone: tz.BaseTzInfo

    user: str
    password: str
    database: str

    host: str = "127.0.0.1"
    port: int = 3306

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

        self.host = configs.get("host", MySqlConnector.host)
        self.port = configs.get_int("port", MySqlConnector.port)

        self.user = configs.get("user")
        self.password = configs.get("password")

        self.database = configs.get("database")

    def connect(self, resources: Resources) -> None:
        self._logger.debug(f"Connecting to MySQL database {self.database}@{self.host}:{self.port}")
        try:
            self._connection = connector.connect(
                host=self.host, port=self.port, user=self.user, passwd=self.password, database=self.database
            )
            self._timezone = self._select_timezone()
            self._tables = self._connect_tables(resources)

        except DatabaseError as e:
            raise ConnectionException(repr(e), connector=self)

    def disconnect(self) -> None:
        if self._connection is not None:
            self._connection.close()

    # noinspection PyTypeChecker
    def _connect_tables(self, resources: Resources) -> Dict[str, MySqlTable]:
        tables = {}
        table_schemas = self._select_table_schemas()
        tables_configs = self.configs.get_section(MySqlTable.SECTION, defaults={})

        for table_name, table_resources in resources.groupby("table"):
            table_configs = tables_configs.get_section(table_name, defaults={
                "index":   tables_configs.get_section("index", defaults={}),
                "columns": tables_configs.get_section("columns", defaults={}),
            })
            table = MySqlTable.from_configs(self, table_name, table_configs, table_resources)
            tables[table.name] = table
            if table.name not in table_schemas:
                if table_configs.get_bool("create", default=True):
                    table.create()
                else:
                    raise ConnectorException(f"Unable to find configured table: {table_name}", connector=self)
            else:
                table_schema = table_schemas[table.name]
                if table.engine is not None and table.engine != table_schema["engine"]:
                    raise ConnectorException(
                        f"Mismatching table engine for configured table '{table_name}': {table_schema['engine']}"
                    )
                column_schemas = self._select_column_schemas(table.name)
                for column in table:
                    if column.name not in column_schemas:
                        # TODO: Implement column creation if configured
                        raise ConnectorException(f"Unable to find configured column: {column.name}", connector=self)
                    # column_schema = column_schemas[column.name]
                    # TODO: Implement column validation

        return tables

    def _select_table_schemas(self, columns=None) -> Dict[str, Dict[str, Any]]:
        # columns = ['table_name', 'table_rows', 'engine', 'create_time', 'update_time']
        if columns is None:
            columns = ["table_name", "engine"]

        cursor = self.connection.cursor()
        cursor.execute(
            f"SELECT {','.join(f'`{c}`' for c in columns)} "
            f"FROM information_schema.tables WHERE `table_schema`='{self.database}'"
        )

        table_schemas = {}
        for table_params in cursor.fetchall():
            table_schema = dict(zip(columns, table_params))
            table_schemas[table_schema["table_name"]] = table_schema
        return table_schemas

    def _select_column_schemas(self, table: str, columns=None) -> Dict[str, Dict[str, Any]]:
        if columns is None:
            columns = ["column_name", "is_nullable", "data_type", "column_key"]

        cursor = self.connection.cursor()
        cursor.execute(
            f"SELECT {','.join(f'`{c}`' for c in columns)} "
            f"FROM information_schema.columns "
            f"WHERE `table_schema`='{self.database}' AND `table_name`='{table}'"
        )

        column_schemas = {}
        for table_params in cursor.fetchall():
            column_schema = dict(zip(columns, table_params))
            column_schemas[column_schema["column_name"]] = column_schema
        return column_schemas

    def _select_timezone(self) -> tz.BaseTzInfo:
        cursor = self.connection.cursor()
        cursor.execute("SELECT @@system_time_zone as tz")
        for row in cursor.fetchall():
            return tz.timezone(row[0])

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

    # noinspection PyTypeChecker
    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> pd.DataFrame:
        data = pd.DataFrame()
        try:
            for table_name, table_resources in resources.groupby("table"):
                if table_name not in self._tables:
                    raise ConnectorException(f"Table '{table_name}' not available", connector=self)

                table = self.get(table_name)
                if start is None and end is None:
                    table_data = table.select_last(table_resources)
                else:
                    table_data = table.select(table_resources, start, end)

                data = data.merge(table_data, how="outer", left_index=True, right_index=True)
        except DatabaseError as e:
            # TODO: Differentiate between syntax- and connection failures.
            raise ConnectionException(e, connector=self)
        return data

    # noinspection PyTypeChecker
    def write(self, data: pd.DataFrame) -> None:
        try:
            for table_name, table_resources in self.resources.groupby("table"):
                if table_name not in self._tables:
                    raise ConnectorException(f"Table '{table_name}' not available", connector=self)
                table = self.get(table_name)
                table.insert(table_resources, data)

        except DatabaseError as e:
            raise ConnectionException(e, connector=self)

    def is_connected(self) -> bool:
        if self._connection is not None:
            return self._connection.is_connected()
        return False
