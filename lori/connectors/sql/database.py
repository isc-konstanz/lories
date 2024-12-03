# -*- coding: utf-8 -*-
"""
lori.connectors.sql.database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Mapping
from typing import Dict, Iterator, Optional

from sqlalchemy import create_engine, inspect, text, MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Column

import pandas as pd
import pytz as tz
from lori.connectors import ConnectionException, ConnectorException, Database, register_connector_type
from lori.connectors.sql import Table
from lori.core import Configurations, Resources
from lori.util import to_timezone


@register_connector_type
class SqlDatabase(Database, Mapping[str, Table]):
    TYPE: str = "sql"

    _connection = None
    _engine = None
    _session = None
    _metadata = None

    _tables: Dict[str, Table] = {}

    dialect: str
    table_schema: str

    host: str
    port: int

    user: str
    password: str
    database: str

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

        self.dialect = configs.get("dialect").lower()

        self.host = configs.get("host")
        self.port = configs.get_int("port")

        self.user = configs.get("user")
        self.password = configs.get("password")

        self.database = configs.get("database")
        self.table_schema = configs.get("table_schema")

    def connect(self, resources: Resources) -> None:
        self._logger.debug(f"Connecting to {self.dialect} database {self.database}@{self.host}:{self.port}")
        try:
            if self.dialect == "mysql":
                prefix = "mysql+pymysql://"

            elif self.dialect == "mariadb":
                prefix = "mariadb+pymysql://"

            elif self.dialect == "postgresql":
                prefix = "postgresql+psycopg2://"
            else:
                raise ValueError("Unsupported database type")

            self._engine = create_engine(
                url=f"{prefix}{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
                pool_recycle=3600,
            )
            session = sessionmaker(bind=self._engine)

            self._metadata = MetaData()
            self._metadata.reflect(bind=self._engine)

            self._connection = session()

            # Make sure the session timezone is UTC
            now = pd.Timestamp.now()
            self._set_timezone(tz.UTC)
            if self._select_timezone().utcoffset(now).seconds != 0:
                raise ConnectorException("Error setting session timezone to UTC")

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
                table_resources,
            )
            tables[table.name] = table

            schema_found = False
            for schema, table_list in table_schemas.items():
                if table.name in table_list:
                    schema_found = True
                    table_schema = schema
                    column_schemas = inspector.get_columns(table.name, table_schema)
                    column_names = [col["name"] for col in column_schemas]

                    for column in table.columns:
                        if column.name not in column_names:
                            if table_configs.get_bool("create_columns", default=True):
                                self._create_column(table.name, column, table_schema)
                            else:
                                raise ConnectorException(
                                    f"Unable to find configured column: {column.name}", connector=self
                                )
                        else:
                            column_schema = next(col for col in column_schemas if col["name"] == column.name)
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
            "postgresql": "SHOW timezone;",
            "mysql": "SELECT @@system_time_zone;",
            "mariadb": "SELECT @@system_time_zone;",
            "sqlite": "SELECT datetime('now');",
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

    def _set_timezone(self, timezone: tz.BaseTzInfo) -> None:
        tz_offset = pd.Timestamp.now(timezone).strftime('%z')
        tz_offset_formatted = tz_offset[:3] + ':' + tz_offset[3:]

        if self.dialect in ('mysql', 'mariadb'):
            query = f"SET time_zone = '{tz_offset_formatted}'"
        elif self.dialect == 'postgresql':
            query = f"SET TIME ZONE '{tz_offset_formatted}'"
        else:
            raise NotImplementedError(f"Timezone setting not implemented for dialect: {self.dialect}")

        self._connection.execute(text(query))
        self._connection.commit()

    def hash(
            self,
            resources: Resources,
            start: Optional[pd.Timestamp | dt.datetime] = None,
            end: Optional[pd.Timestamp | dt.datetime] = None,
            method: str = "MD5",
            encoding: str = "UTF-8",
    ) -> Optional[str]:
        if method.lower() not in ["md5", "sha256"]:
            raise ValueError(f"Invalid checksum method '{method}'")

        table_hashes = []
        try:
            for table_name, table_resources in resources.groupby("table"):
                if table_name not in self._tables:
                    raise ConnectorException(f"Table '{table_name}' not available", connector=self)

                table = self.get(table_name)
                table_hash = table.select_hash(resources, start, end, method=method, encoding=encoding)
                table_hashes.append(table_hash)

        except SQLAlchemyError as e:
            if 'syntax' in str(e).lower():
                raise SyntaxError(f"SQL Syntax Error: {repr(e)}")
            else:
                raise ConnectionException(f"Connection Error: {repr(e)}", connector=self)

        if len(table_hashes) == 0:
            return None
        elif len(table_hashes) == 1:
            return table_hashes[0]

        if method.lower() == "md5":
            return hashlib.md5(",".join(table_hashes).encode(encoding)).hexdigest()
        elif method.lower() == "sha256":
            return hashlib.sha256(",".join(table_hashes).encode(encoding)).hexdigest()

    # noinspection PyTypeChecker
    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
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

                data = pd.concat([data, table_data], axis="index")
                # data = data.merge(table_data, how="outer", left_index=True, right_index=True)
        except SQLAlchemyError as e:
            if 'syntax' in str(e).lower():
                raise SyntaxError(f"SQL Syntax Error: {repr(e)}")
            else:
                raise ConnectionException(f"Connection Error: {repr(e)}", connector=self)
        return data

    # noinspection PyTypeChecker
    def read_first(self, resources: Resources) -> pd.DataFrame:
        data = pd.DataFrame()
        try:
            for table_name, table_resources in resources.groupby("table"):
                if table_name not in self._tables:
                    raise ConnectorException(f"Table '{table_name}' not available", connector=self)

                table_data = self.get(table_name).select_first(table_resources)

                data = data.merge(table_data, how="outer", left_index=True, right_index=True)
        except SQLAlchemyError as e:
            if 'syntax' in str(e).lower():
                raise SyntaxError(f"SQL Syntax Error: {repr(e)}")
            else:
                raise ConnectionException(f"Connection Error: {repr(e)}", connector=self)
        return data

    # noinspection PyTypeChecker
    def read_last(self, resources: Resources) -> pd.DataFrame:
        data = pd.DataFrame()
        try:
            for table_name, table_resources in resources.groupby("table"):
                if table_name not in self._tables:
                    raise ConnectorException(f"Table '{table_name}' not available", connector=self)

                table_data = self.get(table_name).select_last(table_resources)

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
                table_data = data.loc[:, [r.id for r in table_resources if r.id in data.columns]]
                if table_data.empty:
                    continue
                table = self.get(table_name)
                table.insert(table_resources, data)

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
    