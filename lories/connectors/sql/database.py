# -*- coding: utf-8 -*-
"""
lories.connectors.sql.database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict, Iterator, Literal, Mapping, Optional

from sqlalchemy import Connection, Dialect, Engine, create_engine, event, text
from sqlalchemy.exc import SQLAlchemyError

import pandas as pd
import pytz as tz
from lories.connectors import ConnectionError, Database, DatabaseError, register_connector_type
from lories.connectors.sql import Schema, Table
from lories.core.configs import ConfigurationError
from lories.core.configs.parameters import ChannelParameter, Parameter, ParameterGroup, SelectParameter
from lories.data.util import hash_value
from lories.typing import Configurations, Resources, Timestamp
from lories.util import to_timezone


@register_connector_type("sql")
class SqlDatabase(Database, Mapping[str, Table]):
    """
    SQL connector backed by SQLAlchemy, supporting PostgreSQL, MySQL, SQLite, and MSSQL dialects.
    It provides a unified interface for reading and writing time-series data to relational databases,
    with per-table configuration overrides and automatic schema introspection. SQLAlchemy's broad
    dialect support enables portability across database engines, though performance characteristics
    and SQL feature availability vary by backend.
    """

    _host = Parameter(key="host", type=str, required=True, desc="Database server hostname")
    _port = Parameter(key="port", type=int, required=True, min=1, max=65535, desc="Database server TCP port")
    _user = Parameter(key="user", type=str, required=True, desc="Database authentication username")
    _password = Parameter(key="password", type=str, required=True, desc="Database authentication password", secret=True)
    _database = Parameter(key="database", type=str, required=True, desc="Default database / schema name to connect to")
    _dialect = SelectParameter(
        ["postgresql", "mysql", "mariadb", "sqlite", "mssql"],
        key="dialect",
        required=True,
        desc="SQLAlchemy database dialect (selects driver and SQL flavour)",
    )
    _tables = ParameterGroup(
        key="tables",
        required=False,
        desc="Per-table configuration overrides keyed by table name (see lories.connectors.sql.Schema)",
    )

    # Per-channel parameters
    schema = ChannelParameter(type=str, required=False, desc="Database schema name for this channel's table")
    table = ChannelParameter(
        type=str, required=False, desc="Table name for this channel (defaults to the channel group)"
    )

    dialect: Dialect

    host: str
    port: int

    user: str
    password: str
    database: str

    engine: Optional[Engine] = None
    _schema: Schema

    __tables: Dict[str, Table]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__tables = OrderedDict()

    def __iter__(self) -> Iterator[str]:
        return iter(self.__tables)

    def __len__(self) -> int:
        return len(self.__tables)

    def __contains__(self, table: str | Table) -> bool:
        if isinstance(table, str):
            return table in self.__tables.keys()
        if isinstance(table, Table):
            return table in self.__tables.values()
        return False

    def __getitem__(self, name: str) -> Table:
        return self.__tables[name]

    # noinspection PyShadowingBuiltins, PyProtectedMember
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()
        vars.pop("_schema", None)
        if self.is_configured():
            vars["dialect"] = self.dialect.name
            vars["host"] = self.host
            vars["port"] = self.port
            vars["user"] = self.user
            vars["database"] = self.database
        vars["tables"] = f"[{', '.join(c.name for c in self.__tables.values())}]"
        return vars

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self.host = self._host
        self.port = self._port

        self.user = self._user
        self.password = self._password

        self.database = self._database

        dialect = self._dialect.lower()
        if dialect == "mysql":
            prefix = "mysql+pymysql://"
        elif dialect == "mariadb":
            prefix = "mariadb+pymysql://"
        elif dialect == "postgresql":
            prefix = "postgresql+psycopg2://"
        else:
            raise ConfigurationError(f"Unsupported database type: {dialect}")
        try:
            self.engine = create_engine(
                url=f"{prefix}{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
                pool_recycle=-1,
                pool_pre_ping=True,
            )
            self.dialect = self.engine.dialect

            # Every pooled connection is pinned to UTC on creation, so each
            # per-operation checkout sees the same timezone the removed
            # long-lived connection used to be configured with.
            timezone_query = self._utc_timezone_query(self.dialect.name)

            @event.listens_for(self.engine, "connect")
            def _set_connection_timezone(dbapi_connection, connection_record) -> None:
                self._pin_connection_timezone(dbapi_connection, timezone_query)

            self._schema = Schema(self.dialect)
            self._schema.configure(configs.get_member("tables", defaults={}))

        except SQLAlchemyError as e:
            raise ConfigurationError(f"Unable to create database engine: {str(e)}")

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        self._logger.debug(f"Connecting to {self.dialect.name} database {self.database}@{self.host}:{self.port}")
        try:
            # Check out one real pooled connection so credential, connectivity
            # and timezone errors surface here instead of at the first read.
            with self.engine.connect() as connection:
                now = pd.Timestamp.now()
                if self._select_timezone(connection).utcoffset(now).seconds != 0:
                    raise DatabaseError(self, "Error setting connection timezone to UTC")

            self.__tables = self._schema.connect(self.engine, resources)

        except SQLAlchemyError as e:
            self._raise(e)

    def disconnect(self) -> None:
        super().disconnect()
        if self.engine is not None:
            self.engine.dispose()
            self._logger.debug("Disconnected from the database")

    @staticmethod
    def _pin_connection_timezone(dbapi_connection, timezone_query: str) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(timezone_query)
        finally:
            cursor.close()

    @staticmethod
    def _utc_timezone_query(dialect: str) -> str:
        if dialect == "postgresql":
            return "SET TIME ZONE '+00:00'"
        elif dialect in ("mysql", "mariadb"):
            return "SET time_zone = '+00:00'"
        else:
            raise NotImplementedError(f"Timezone setting not implemented for dialect: {dialect}")

    def _select_timezone(self, connection: Connection) -> tz.BaseTzInfo:
        if self.dialect.name == "postgresql":
            query = "SHOW TIMEZONE"
        elif self.dialect.name in ("mariadb", "mysql"):
            query = "SELECT @@session.time_zone"
        # elif self.dialect.name == 'sqlite':
        #     query = "SELECT datetime('now')"
        else:
            raise NotImplementedError(f"Timezone setting not implemented for dialect: {self.dialect.name}")
        try:
            result = connection.execute(text(query))
            timezone = result.scalar()
            return to_timezone(timezone)
        except KeyError:
            raise ValueError(f"Unsupported database type: {self.dialect.name}")
        except SQLAlchemyError as e:
            raise RuntimeError(f"Error fetching timezone: {e}")

    def hash(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        method: Literal["MD5", "SHA1", "SHA256", "SHA512"] = "MD5",
        encoding: str = "UTF-8",
    ) -> Optional[str]:
        hashes = []
        try:
            with self.engine.connect() as connection:
                for table_schema, schema_resources in resources.groupby("schema"):
                    for table_name, table_resources in schema_resources.groupby(
                        lambda c: c.get("table", default=c.group)
                    ):
                        if table_name not in self.__tables:
                            raise DatabaseError(self, f"Table '{table_name}' not available")

                        table = self.get(table_name)
                        select = table.hash(table_resources, start, end, method=method)
                        result = connection.execute(select)

                        # noinspection PyTypeChecker
                        if result.rowcount < 1:
                            continue

                        table_hashes = [r[0] for r in result.fetchall()]
                        if len(table_hashes) > 1:
                            table_hash = hash_value(",".join(table_hashes), method, encoding)
                        else:
                            table_hash = table_hashes[0]
                        hashes.append(table_hash)

        except SQLAlchemyError as e:
            self._raise(e)

        if len(hashes) == 0:
            return None
        elif len(hashes) == 1:
            return hashes[0]

        return hash_value(",".join(hashes), method, encoding)

    def exists(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> bool:
        try:
            with self.engine.connect() as connection:
                for table_schema, schema_resources in resources.groupby("schema"):
                    for table_name, table_resources in schema_resources.groupby(
                        lambda c: c.get("table", default=c.group)
                    ):
                        if table_name not in self.__tables:
                            raise DatabaseError(self, f"Table '{table_name}' not available")

                        table = self.get(table_name)
                        select = table.exists(table_resources, start, end)
                        result = connection.execute(select)

                        # noinspection PyTypeChecker
                        if result.rowcount < 1:
                            continue
                        count = result.scalar()
                        if count is None or int(count) > 1:
                            return True
        except SQLAlchemyError as e:
            self._raise(e)
        return False

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def read(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame:
        results = []
        try:
            with self.engine.connect() as connection:
                for table_schema, schema_resources in resources.groupby("schema"):
                    for table_name, table_resources in schema_resources.groupby(
                        lambda c: c.get("table", default=c.group)
                    ):
                        table_key = table_name if table_schema is None else f"{table_schema}.{table_name}"
                        if table_key not in self.__tables:
                            raise DatabaseError(self, f"Table '{table_key}' not available")

                        table = self.get(table_key)
                        if start is None and end is None:
                            select = table.read(table_resources, order_by="desc").limit(1)
                        else:
                            select = table.read(table_resources, start, end)

                        result = connection.execute(select)
                        if result.rowcount > 0:
                            result_data = table.extract(table_resources, result)
                            if not result_data.empty:
                                results.append(result_data)
        except SQLAlchemyError as e:
            self._raise(e)

        if len(results) == 0:
            return pd.DataFrame()
        results = sorted(results, key=lambda d: min(d.index))
        return pd.concat(results, axis="columns")

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def read_first(self, resources: Resources) -> pd.DataFrame:
        results = []
        try:
            with self.engine.connect() as connection:
                for table_schema, schema_resources in resources.groupby("schema"):
                    for table_name, table_resources in schema_resources.groupby(
                        lambda c: c.get("table", default=c.group)
                    ):
                        if table_name not in self.__tables:
                            raise DatabaseError(self, f"Table '{table_name}' not available")

                        table = self.get(table_name)
                        select = table.read(table_resources, order_by="asc").limit(1)
                        result = connection.execute(select)
                        if result.rowcount > 0:
                            result_data = table.extract(table_resources, result)
                            if not result_data.empty:
                                results.append(result_data)
        except SQLAlchemyError as e:
            self._raise(e)

        if len(results) == 0:
            return pd.DataFrame()
        results = sorted(results, key=lambda d: min(d.index))
        return pd.concat(results, axis="columns")

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def read_last(self, resources: Resources) -> pd.DataFrame:
        results = []
        try:
            with self.engine.connect() as connection:
                for table_schema, schema_resources in resources.groupby("schema"):
                    for table_name, table_resources in schema_resources.groupby(
                        lambda c: c.get("table", default=c.group)
                    ):
                        if table_name not in self.__tables:
                            raise DatabaseError(self, f"Table '{table_name}' not available")

                        table = self.get(table_name)
                        select = table.read(table_resources, order_by="desc").limit(1)
                        result = connection.execute(select)
                        if result.rowcount > 0:
                            result_data = table.extract(table_resources, result)
                            if not result_data.empty:
                                results.append(result_data)
        except SQLAlchemyError as e:
            self._raise(e)

        if len(results) == 0:
            return pd.DataFrame()
        results = sorted(results, key=lambda d: min(d.index))
        return pd.concat(results, axis="columns")

    # noinspection PyTypeChecker
    def write(self, data: pd.DataFrame) -> None:
        try:
            # One transaction per call: all table inserts of this batch commit
            # together on scope exit or roll back together on error.
            with self.engine.begin() as connection:
                for table_schema, schema_resources in self.resources.groupby("schema"):
                    for table_name, table_resources in schema_resources.groupby(
                        lambda c: c.get("table", default=c.group)
                    ):
                        if table_name not in self.__tables:
                            raise DatabaseError(self, f"Table '{table_name}' not available")
                        table_data = data.loc[:, [r.id for r in table_resources if r.id in data.columns]]
                        table_data = table_data.dropna(axis="index", how="all")
                        if table_data.empty:
                            continue
                        table = self.get(table_name)
                        insert = table.write(table_resources, table_data)
                        self._logger.debug(insert)
                        connection.execute(insert)

        except SQLAlchemyError as e:
            self._raise(e)

    def delete(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> None:
        try:
            with self.engine.begin() as connection:
                for table_schema, schema_resources in resources.groupby("schema"):
                    for table_name, table_resources in schema_resources.groupby(
                        lambda c: c.get("table", default=c.group)
                    ):
                        if table_name not in self.__tables:
                            raise DatabaseError(self, f"Table '{table_name}' not available")
                        table = self.get(table_name)
                        delete = table.delete(table_resources, start, end)
                        self._logger.debug(delete)
                        connection.execute(delete)

        except SQLAlchemyError as e:
            self._raise(e)

    def is_connected(self) -> bool:
        if self.engine is None:
            return False
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError:
            return False

    def _raise(self, e: SQLAlchemyError):
        if "syntax" in str(e).lower():
            raise DatabaseError(self, str(e))
        else:
            raise ConnectionError(self, str(e))
