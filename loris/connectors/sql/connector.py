# -*- coding: utf-8 -*-
"""
loris.connectors.sql.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Dict, Iterator, Optional

from sqlalchemy import create_engine, inspect, text, MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Column

import pandas as pd
import pytz as tz

from loris.connectors import ConnectionException, Connector, ConnectorException, register_connector_type
from loris.connectors.sql import Table
from loris.core import Configurations, Resources
from loris.util import to_timezone


@register_connector_type
class SqlConnector(Connector, Mapping[str, Table]):
    TYPE: str = "sql"

    _connection = None
    _engine = None
    _session = None
    _metadata = None

    _tables: Dict[str, Table] = {}

    _timezone: tz.BaseTzInfo

    dialect: str
    table_schema: str

    user: str
    password: str
    database: str

    host: str
    port: int

    @property
    def connection(self):
        if self._connection is None:
            raise ConnectionException("SQLAlchemy Connection not open", connector=self)
        return self._connection

    def __getitem__(self, table_name: str) -> Table:
        return self._tables[table_name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._tables)

    def __len__(self) -> int:
        return len(self._tables)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self.dialect = configs.get("dialect")

        self.host = configs.get("host")
        self.port = configs.get_int("port")

        self.user = configs.get("user")
        self.password = configs.get("password")

        self.database = configs.get("database")
        self.table_schema = configs.get("table_schema")

    def create_engine(self):
        if self.dialect == "mysql" or self.dialect == "mariadb":
            connection_prefix = "mysql+pymysql://"
        elif self.dialect == "postgres":
            connection_prefix = "postgresql+psycopg2://"
        else:
            raise ValueError("Unsupported database type")

        self._engine = create_engine(
            f"{connection_prefix}{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

        self._metadata = MetaData()
        self._metadata.reflect(bind=self._engine)

        self._session = sessionmaker(bind=self._engine)
        self._connection = self._session()

    def connect(self, resources: Resources) -> None:
        self._logger.debug(f"Connecting to {self.dialect.upper()} database {self.database}@{self.host}:{self.port}")
        try:
            self.create_engine()
            self._timezone = self._select_timezone()
            self._tables = self._connect_tables(resources)

        except SQLAlchemyError as e:
            self._logger.error(f"Connection failed: {e}")
            raise ConnectionException(repr(e), connector=self)

    def disconnect(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._logger.debug("Disconnected from the database")

    def _connect_tables(self, resources: Resources) -> Dict[str, Table]:
        tables = {}
        inspector = inspect(self._engine)
        schemas = [self.table_schema] if self.table_schema else inspector.get_schema_names()
        table_schemas = {schema: inspector.get_table_names(schema) for schema in schemas}
        tables_configs = self.configs.get_section(Table.SECTION, defaults={})
        tables_defaults = {
            "index": tables_configs.get_section("index", defaults={}),
            "columns": tables_configs.get_section("columns", defaults={}),
        }

        for table_name, table_resources in resources.groupby("table"):
            table_configs = tables_configs.get_section(table_name, defaults=tables_defaults)
            table = Table.from_configs(
                self,
                self._engine,
                self._metadata,
                self.table_schema,
                table_name,
                table_configs,
                table_resources
            )
            tables[table.name] = table

            schema_found = False
            for schema, table_list in table_schemas.items():
                if table.name in table_list:
                    schema_found = True
                    table_schema = schema
                    column_schemas = inspector.get_columns(table.name, table_schema)
                    column_names = [col['name'] for col in column_schemas]

                    for column in table.columns:
                        if column.name not in column_names:
                            if table_configs.get_bool("create_columns", default=True):
                                self._create_column(table.name, column, table_schema)
                            else:
                                raise ConnectorException(
                                    f"Unable to find configured column: {column.name}",
                                    connector=self
                                )
                        else:
                            column_schema = next(col for col in column_schemas if col['name'] == column.name)
                            # TODO: Implement column validation
                    break

            if not schema_found:
                if table_configs.get_bool("create", default=True):
                    table.create()
                else:
                    raise ConnectorException(f"Unable to find configured table: {table_name}", connector=self)

        return tables

    def _create_column(self, table_name: str, column: Column, schema: str):
        query = f"ALTER TABLE {schema}.{table_name} ADD COLUMN {column.name} {column.type}"
        self.connection.execute(text(query))

    def _select_timezone(self) -> tz.BaseTzInfo:
        timezone_queries = {
            'postgresql': "SHOW timezone;",
            'mysql': "SELECT @@system_time_zone;",
            'mariadb': "SELECT @@system_time_zone;",
            'sqlite': "SELECT datetime('now');"
        }
        try:
            query = timezone_queries[self._engine.dialect.name]
            result = self.connection.execute(text(query))
            timezone = result.scalar()
            return to_timezone(timezone)
        except KeyError:
            raise ValueError(f"Unsupported database type: {self._engine.dialect.name}")
        except SQLAlchemyError as e:
            raise RuntimeError(f"Error fetching timezone: {e}")

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

            return data
        except SQLAlchemyError as e:
            self._logger.error(f"Read failed: {e}")
            raise ConnectionException(repr(e), connector=self)

    # noinspection PyTypeChecker
    def write(self, data: pd.DataFrame) -> None:
        try:
            for table_name, table_resources in self.resources.groupby("table"):
                if table_name not in self._tables:
                    raise ConnectorException(f"Table '{table_name}' not available", connector=self)
                table = self.get(table_name)
                table.insert(table_resources, data)
                self._connection.commit()

        except SQLAlchemyError as e:
            self._logger.error(f"Write failed: {e}")
            raise ConnectionException(repr(e), connector=self)

    def is_connected(self) -> bool:
        if self._connection is not None:
            try:
                self._connection.execute(text("SELECT 1"))
                return True
            except SQLAlchemyError:
                return False
        return False
