# -*- coding: utf-8 -*-
"""
lories.connectors.sql.timescale
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from sqlalchemy import Connection, text
from sqlalchemy.exc import SQLAlchemyError

from lories.connectors import DatabaseError, register_connector_type
from lories.connectors.sql import SqlDatabase, Table
from lories.connectors.sql.columns import DatetimeColumn
from lories.core.configs import ConfigurationError
from lories.typing import Configurations, Resources


@register_connector_type("timescale", "timescaledb")
class TimescaleDatabase(SqlDatabase):
    """
    TimescaleDB connector: PostgreSQL with hypertable conversion at connect time.
    All read, write, upsert and delete paths are inherited unchanged from
    :class:`SqlDatabase`; this subclass pins the dialect to PostgreSQL and converts
    the connected tables to hypertables partitioned by their datetime primary key.
    """

    chunk_interval: str
    create_extension: bool
    migrate_data: bool

    def configure(self, configs: Configurations) -> None:
        configs.set("dialect", "postgresql", replace=False)
        dialect = configs.get("dialect")
        if dialect.lower() != "postgresql":
            raise ConfigurationError(f"TimescaleDB requires the 'postgresql' dialect, not: {dialect}")
        super().configure(configs)

        self.chunk_interval = configs.get("chunk_interval", default="7 days")
        self.create_extension = configs.get_bool("create_extension", default=True)
        self.migrate_data = configs.get_bool("migrate_data", default=False)

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        try:
            with self.engine.begin() as connection:
                self._create_hypertables(connection)
        except SQLAlchemyError as e:
            self._raise(e)

    def _create_hypertables(self, connection: Connection) -> None:
        if self.create_extension:
            connection.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
        for name in self:
            self._create_hypertable(connection, self[name])

    def _create_hypertable(self, connection: Connection, table: Table) -> None:
        time_columns = [c for c in table.primary_key.columns if isinstance(c, DatetimeColumn)]
        if len(time_columns) == 0:
            raise DatabaseError(self, f"Table '{table.key}' has no datetime primary key to partition by")

        query = text(
            "SELECT create_hypertable("
            ":table, :column,"
            " chunk_time_interval => CAST(:interval AS INTERVAL),"
            " if_not_exists => TRUE,"
            " migrate_data => :migrate)"
        )
        connection.execute(
            query,
            {
                "table": table.key,
                "column": time_columns[0].name,
                "interval": self.chunk_interval,
                "migrate": self.migrate_data,
            },
        )
