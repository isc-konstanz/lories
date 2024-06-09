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
import pytz as tz
from loris.channels import Channels, ChannelState
from loris.configs import Configurations
from loris.connectors import ConnectionException, Connector, ConnectorException
from loris.connectors.mysql import MySqlColumn, MySqlTable
from loris.util import resample


class MySqlConnector(Connector, Mapping[str, MySqlTable]):
    TYPE: str = "mysql"

    _connection = None
    _tables: Dict[str, MySqlTable]

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

    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        table_configs = configs.get_section(MySqlTable.SECTION, default={})
        self._table_index_column = table_configs.get("index_column", default=MySqlTable.DEFAULT_INDEX_COLUMN)
        self._table_index_type = table_configs.get("index_type", default=MySqlTable.DEFAULT_INDEX_TYPE)
        self._table_data_column = table_configs.get("data_column", default=MySqlTable.DEFAULT_DATA_COLUMN)
        self._table_data_type = table_configs.get("data_type", default=MySqlTable.DEFAULT_DATA_TYPE)
        self._tables_create = table_configs.get_bool("create", default=True)
        self._tables = {}

        # TODO: Validate if minutely default resolution is sufficient
        resolution = configs.get_int("resolution", default=None)
        if resolution is not None:
            resolution *= 60
        self.resolution = resolution

        timezone = configs.get("timezone", None)
        if isinstance(timezone, str):
            timezone = tz.timezone(timezone)
        self.timezone = timezone

        self.host = configs.get("host", "127.0.0.1")
        self.port = configs.get_int("port", 3306)

        self.user = configs.get("user")
        self.password = configs.get("password")

        self.database = configs.get("database")

    def __connect__(self, channels: Channels) -> None:
        self._logger.info(f"Connecting to MySQL database {self.database}@{self.host}:{self.port}")
        self._connection = connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password,
            database=self.database
        )
        self._tables = self._load_tables(channels)

    def __disconnect__(self) -> None:
        if self._connection is not None:
            self._connection.close()

    def _load_tables(self, channels: Channels) -> Dict[str, MySqlTable]:
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
                self,
                table_schema["table_name"],
                table_index,
                table_columns,
                engine=table_schema["engine"]
            )
            tables[table.name] = table

        for table_name, table_channels in channels.groupby("table"):
            if table_name in tables:
                continue
            if self._tables_create:
                table_index = MySqlColumn(
                    self._table_index_column,
                    self._table_index_type,
                    primary=True,
                    nullable=False
                )
                table_columns = []
                for table_channel in table_channels:
                    table_column_name = table_channel.id if "column" not in table_channel else table_channel.column
                    table_column_args = {}
                    if "nullable" in table_channel:
                        table_column_args["nullable"] = table_channel.nullable
                    if "primary" in table_channel:
                        table_column_args["primary"] = table_channel.primary
                    if "value_length" in table_channel:
                        table_column_args["type_length"] = table_channel.value_length

                    table_column_type = (
                        table_channel.value_type if "value_type" in table_channel else self._table_data_type
                    )
                    table_columns.append(MySqlColumn(table_column_name, table_column_type, **table_column_args))

                table = MySqlTable(self, table_name, table_index, table_columns)
                table.create()
                tables[table.name] = table
            else:
                raise ConnectorException(f"Unable to find configured table: {table_name}", connector=self)

        return tables

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
        engine: str = None  # 'MyISAM'
    ) -> MySqlTable:
        table = MySqlTable(self, name, columns, engine)
        table.create()

        self._tables[table.name] = table
        return table

    def exists(
        self,
        channels: Optional[Channels] = None,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> bool:
        # TODO: Replace this placeholder more resource efficient
        if channels is None:
            channels = self._channels
        containers = channels.copy()
        self.read(containers, start, end)
        return not containers.to_frame().empty

    def read(
        self,
        channels: Channels,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> None:
        for table_name, table_channels in channels.groupby("table"):
            if table_name not in self._tables:
                raise ConnectorException(f"Table '{table_name}' not available", connector=self)

            table_columns = [c.id if "column" not in c else c.column for c in table_channels]
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

            for table_channel in table_channels:
                table_channel_column = table_channel.id if "column" not in table_channel else table_channel.column
                if len(table_data.index) > 1:
                    table_channel_data = table_data.loc[:, table_channel_column]
                    table_channel.set(table_data.index[0], table_channel_data)

                elif len(table_data.index) > 0:
                    timestamp = table_data.index[-1]
                    table_channel_data = table_data.loc[timestamp, table_channel_column]
                    table_channel.set(timestamp, table_channel_data)

                else:
                    table_channel.state = ChannelState.NOT_AVAILABLE
                    self._logger.warning(
                        f"Unable to read nonexisting column of table '{table_name}': {table_channel_column}"
                    )

    def write(self, channels: Channels) -> None:
        for table_name, table_channels in self._channels.groupby("table"):
            if table_name not in self._tables:
                raise ConnectorException(f"Table '{table_name}' not available", connector=self)

            table_columns = {c.column: c.id for c in table_channels if "column" in c}

            table = self.get(table_name)
            table.insert(table_channels.to_frame().rename(columns=table_columns))

    def is_connected(self) -> bool:
        if self._connection is not None:
            return self._connection.is_connected()
        return False
