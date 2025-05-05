# -*- coding: utf-8 -*-
"""
lori.connectors.sql.schema
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Collection, Dict, Iterable

from sqlalchemy import Connection, Dialect, Engine, MetaData, inspect

import pytz as tz
from lori.connectors.sql.columns import (
    Column,
    DatetimeColumn,
    SurrogateKeyColumn,
    parse_type,
)
from lori.connectors.sql.columns.datetime import is_datetime
from lori.connectors.sql.index import DatetimeIndexType
from lori.connectors.sql.table import Table
from lori.core import ConfigurationException, Configurations, Configurator, Resource, ResourceException, Resources
from lori.util import to_bool, to_timezone


class Schema(Configurator, MetaData):
    dialect: Dialect

    _created: bool = False

    def __init__(self, dialect: Dialect, **kwargs) -> None:
        super().__init__(**kwargs)
        self._created = False
        self.dialect = dialect

    def __repr__(self) -> str:
        # Do not use __repr__ of Configurator class, to avoid infinite recursion
        return super(MetaData, self).__repr__()

    def __str__(self) -> str:
        # Do not use __str__ of Configurator class, to avoid infinite recursion
        return super(MetaData, self).__str__()

    def connect(self, bind: Engine | Connection, resources: Resources) -> Dict[str, Table]:
        tables = self._connect_tables(resources)
        if not self._created:
            self.create_all(bind=bind, checkfirst=True)
            self._created = True
        self._validate(bind=bind, tables=tables.values())
        return tables

    def _connect_tables(self, resources: Resources) -> Dict[str, Table]:
        tables = {}

        defaults = self.configs.get_sections(["index", "columns"], ensure_exists=True)
        for schema, schema_resources in resources.groupby("schema"):
            for name, table_resources in schema_resources.groupby(lambda c: c.get("table", default=c.group)):
                if name is None:
                    raise ConfigurationException(
                        "Missing 'table' configuration for resources: " + ", ".join([r.id for r in table_resources])
                    )
                if name in self.tables:
                    table = self.tables[name]
                    tables[table.key] = table
                    continue

                configs = self.configs.get_section(name, defaults=defaults)
                columns_configs = configs.get_section("columns")
                column_configs = [columns_configs[s] for s in columns_configs.sections]

                def _filter_primary(primary: bool) -> Collection[Resource | Configurations]:
                    _filter_columns = [*column_configs, *table_resources]
                    return [r for r in _filter_columns if r.get("primary", default=False) == primary]

                columns = []
                columns.extend(Schema._create_primary_key(configs.get_section("index"), *_filter_primary(True)))
                columns.extend(Schema._create_columns(*_filter_primary(False)))

                def _filter_duplicates() -> Collection[Column]:
                    unique = set()
                    return [c for c in columns if (c.name in unique or unique.add(c.name))]

                # Remove duplicate columns that will be created for tables with surrogate keys,
                # which will have duplicate resource configurations.
                for duplicate in _filter_duplicates():
                    if not any(self._validate_column(column, duplicate) for column in columns):
                        raise ConfigurationException(f"Duplicate column for table '{name}': {duplicate}")
                    columns.remove(duplicate)

                table = Table(name, self, *columns, schema=schema, quote=True, quote_schema=True)
                tables[table.key] = table
        return tables

    # noinspection PyShadowingBuiltins
    @staticmethod
    def _create_primary_key(configs: Configurations, *resources: Resource | Configurations) -> Iterable[Column]:
        columns = []
        type = configs.get("type", default="default")
        if type.lower() in ["default", "none"]:
            type = DatetimeIndexType.TIMESTAMP
            columns = type.columns(configs.get("column", default=None))
        elif type.lower() != "custom":
            type = DatetimeIndexType.get(type.upper())
            columns = type.columns(configs.get("column", default=None))

        columns.extend(Schema._create_columns(*resources))
        return columns

    @staticmethod
    def _create_columns(*resources: Resource | Configurations) -> Iterable[Column]:
        return [Schema._create_column(resource) for resource in resources]

    # noinspection PyShadowingBuiltins
    @staticmethod
    def _create_column(resource: Resource | Configurations) -> Column:
        name = resource.get("column", default=resource.key)
        type = parse_type(resource["type"], resource.get("length", None))
        primary = to_bool(resource.get("primary", default=False))
        configs = {
            "primary_key": primary,
            "default": resource.get("default", default=None),
            "nullable": resource.get("nullable", default=True if not primary else False),
        }
        if primary:
            attribute = resource.get("attribute", default=None)
            if attribute is not None and len(attribute) > 0:
                return SurrogateKeyColumn(name, type, attribute, **configs)

        if is_datetime(type):
            configs["timezone"] = to_timezone(resource.get("timezone", default=tz.UTC)) if not primary else tz.UTC
            return DatetimeColumn(name, type, **configs)

        return Column(name, type, **configs)

    def _validate(self, bind: Engine | Connection, tables: Collection[Table]) -> None:
        self.reflect(bind)

        inspector = inspect(bind)
        for table in tables:
            columns = inspector.get_columns(table.name, table.schema)
            column_names = [c["name"] for c in columns]
            for column in table.columns.values():
                if column.name in column_names:
                    column_schema = next(c for c in columns if c["name"] == column.name)  # noqa F841
                    # TODO: Implement column validation
                else:
                    if self.configs.get_bool("create", default=True):
                        raise ResourceException(f"Not yet implemented to create missing column: {column.name}")
                    else:
                        raise ResourceException(f"Unable to find configured column: {column.name}")

    @staticmethod
    def _validate_column(column: Column, other: Column) -> bool:
        return (
            column.name == other.name
            and column.type == other.type
            and column.nullable == column.nullable
            and column.primary_key == other.primary_key
            and column.server_default == column.server_default
            and column.server_onupdate == column.server_onupdate
        )
