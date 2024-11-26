# -*- coding: utf-8 -*-
"""
lori.connectors.sql.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any, Dict, Iterator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

import pandas as pd
import pytz as tz
from lori.connectors import ConnectionException, Connector, ConnectorException, register_connector_type
from lori.connectors.sql import Table
from lori.core import Configurations, Resources
from lori.util import to_timezone


@register_connector_type
class SqlConnector(Connector, Mapping[str, Table]):
    TYPE: str = "sql"

    _connection = None
    _engine = None
    _session = None

    _tables: Dict[str, Table] = {}

    _timezone: tz.BaseTzInfo
    db_type: str

    user: str
    password: str
    database: str

    host: str
    port: int

    @property
    def connection(self):
        if self._connection is None:
            raise ConnectionException(self, "SQLAlchemy Connection not open")
        return self._connection

    def __getitem__(self, table_name: str) -> Table:
        return self._tables[table_name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._tables)

    def __len__(self) -> int:
        return len(self._tables)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self.db_type = configs.get("db_type")

        self.host = configs.get("host")
        self.port = configs.get_int("port")

        self.user = configs.get("user")
        self.password = configs.get("password")

        self.database = configs.get("database")

    def create_engine(self):
        if self.db_type == "mysql" or self.db_type == "mariadb":
            connection_prefix = "mysql+mysqlconnector://"
        elif self.db_type == "postgres":
            connection_prefix = "postgresql+psycopg2://"
        else:
            raise ValueError("Unsupported database type")

        self._engine = create_engine(
            f"{connection_prefix}{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

        self._session = sessionmaker(bind=self._engine)
        self._connection = self._session()

    def connect(self, resources: Resources) -> None:
        self._logger.debug(f"Connecting to {self.db_type.upper()} database {self.database}@{self.host}:{self.port}")
        try:
            self.create_engine()
            self._timezone = self._select_timezone()
            self._tables = self._connect_tables(resources)

        except SQLAlchemyError as e:
            raise ConnectionException(self, repr(e))

    def disconnect(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    # noinspection PyTypeChecker
    def _connect_tables(self, resources: Resources) -> Dict[str, Table]:
        tables = {}
        table_schemas = self._select_table_schemas()
        tables_configs = self.configs.get_section(Table.SECTION, defaults={})
        tables_defaults = {
            "index": tables_configs.get_section("index", defaults={}),
            "columns": tables_configs.get_section("columns", defaults={}),
        }

        for table_name, table_resources in resources.groupby("table"):
            table_configs = tables_configs.get_section(table_name, defaults=tables_defaults)
            table = Table.from_configs(self, table_name, table_configs, table_resources)
            tables[table.name] = table
            if table.name not in table_schemas:
                if table_configs.get_bool("create", default=True):
                    table.create()
                else:
                    raise ConnectorException(self, f"Unable to find configured table: {table_name}")
            else:
                table_schema = table_schemas[table.name]
                if table.engine is not None and table.engine != table_schema["engine"]:
                    raise ConnectorException(
                        self,
                        f"Mismatching table engine for configured table '{table_name}': {table_schema['engine']}",
                    )

                column_schemas = self._select_column_schemas(table.name)
                for column in table:
                    if column.name not in column_schemas:
                        # TODO: Implement column creation if configured
                        raise ConnectorException(self, f"Unable to find configured column: {column.name}")
                    # column_schema = column_schemas[column.name]
                    # TODO: Implement column validation

        return tables

    def _select_table_schemas(self, columns=None) -> Dict[str, Dict[str, Any]]:
        # columns = ['table_name', 'table_rows', 'engine', 'create_time', 'update_time']
        if columns is None:
            columns = ["table_name"]
            if self.db_type == "mysql" or self.db_type == "mariadb":
                columns.append("engine")

        query = (
            f"SELECT {','.join(f'`{c}`' for c in columns)} FROM information_schema.tables "
            f"WHERE `table_schema`=:database"
        )
        query = query.replace("`", '"') if self.db_type == "postgres" else query
        result = self._connection.execute(text(query), {"database": self.database})

        table_schemas = {}
        for table_params in result.fetchall():
            table_schema = dict(zip(columns, table_params))
            table_schemas[table_schema["table_name"]] = table_schema

        return table_schemas

    def _select_column_schemas(self, table: str, columns=None) -> Dict[str, Dict[str, Any]]:
        if columns is None:
            columns = ["column_name", "is_nullable", "data_type", "column_key"]

        query = (
            f"SELECT {','.join(f'`{c}`' for c in columns)} FROM information_schema.columns "
            f"WHERE `table_schema`=:database AND `table_name`=:table"
        )
        query = query.replace("`", '"') if self.db_type == "postgres" else query
        result = self._connection.execute(text(query), {"database": self.database, "table": table})

        column_schemas = {}
        for row in result.fetchall():
            column_schema = dict(zip(columns, row))
            column_schemas[column_schema["column_name"]] = column_schema

        return column_schemas

    def _select_timezone(self) -> tz.BaseTzInfo:
        if self.db_type == "mysql" or self.db_type == "mariadb":
            query = "SELECT @@system_time_zone as tz"
        elif self.db_type == "postgres":
            query = "SHOW TIMEZONE"
        else:
            raise ValueError("Unsupported database type")

        result = self._connection.execute(text(query))
        timezone = result.scalar()
        return to_timezone(timezone)

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
                    raise ConnectorException(self, f"Table '{table_name}' not available")

                table = self.get(table_name)
                if start is None and end is None:
                    table_data = table.select_last(table_resources)
                else:
                    table_data = table.select(table_resources, start, end)

                data = data.merge(table_data, how="outer", left_index=True, right_index=True)
        except SQLAlchemyError as e:
            # TODO: Differentiate between syntax- and connection failures.
            raise ConnectionException(self, repr(e))
        return data

    # noinspection PyTypeChecker
    def write(self, data: pd.DataFrame) -> None:
        try:
            for table_name, table_resources in self.resources.groupby("table"):
                if table_name not in self._tables:
                    raise ConnectorException(self, f"Table '{table_name}' not available")
                table = self.get(table_name)
                table.insert(table_resources, data)

        except SQLAlchemyError as e:
            raise ConnectionException(self, repr(e))

    def is_connected(self) -> bool:
        if self._connection is not None:
            try:
                self._connection.execute(text("SELECT 1"))
                return True
            except SQLAlchemyError:
                return False
        return False
