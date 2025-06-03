# -*- coding: utf-8 -*-
"""
lori.connectors.sql.table
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Tuple

import sqlalchemy as sql
from sqlalchemy import ClauseElement, Dialect, Result, UnaryExpression
from sqlalchemy.sql import Delete, Insert, Select, and_, asc, between, desc, func, literal, not_, text
from sqlalchemy.types import BLOB, DATETIME, TIMESTAMP

import numpy as np
import pandas as pd
import pytz as tz
from lori.connectors.sql.columns import Column, DatetimeColumn, SurrogateKeyColumn
from lori.connectors.sql.index import DatetimeIndexType
from lori.core import Resource, ResourceException, Resources
from lori.typing import TimestampType

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class Table(sql.Table):
    def __init__(
        self,
        name: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(name, *args, **kwargs)
        self.datetime_index_type = DatetimeIndexType.NONE
        for datetime_index_type in [
            DatetimeIndexType.DATE_AND_TIME,
            DatetimeIndexType.DATETIME,
            DatetimeIndexType.TIMESTAMP,
            DatetimeIndexType.TIMESTAMP_UNIX,
        ]:
            if self.__has_datetime_index(datetime_index_type):
                self.datetime_index_type = datetime_index_type
                break

    # noinspection PyUnresolvedReferences
    @property
    def dialect(self) -> Dialect:
        return self.metadata.dialect

    @property
    def primary_index(self) -> Column:
        return self.primary_key.columns[0]

    def _primary_order(self, order: Literal["asc", "desc"] = "asc") -> List[UnaryExpression]:
        clauses = []
        for primary_key in reversed(self.primary_key.columns):
            # if isinstance(primary_key, SurrogateKeyColumn):
            #     continue
            if order == "asc":
                clauses.append(asc(primary_key))
            elif order == "desc":
                clauses.append(desc(primary_key))
            else:
                raise ValueError(f"Unknown order '{order}'")
        return clauses

    def _primary_clauses(
        self,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> List[ClauseElement]:
        clauses = []
        primary_index = self.primary_index
        if self.__is_datetime_index(primary_index):
            if start is not None and end is not None:
                clauses.append(between(primary_index, primary_index.validate(start), primary_index.validate(end)))
            elif start is not None:
                clauses.append(primary_index >= primary_index.validate(start))
            elif end is not None:
                clauses.append(primary_index <= primary_index.validate(end))
        return clauses

    def extract(self, resources: Resources, result: Result[Any]) -> pd.DataFrame:
        result_columns = [r.id for r in resources]
        results = []

        # noinspection PyUnresolvedReferences
        if result.rowcount < 1:
            return pd.DataFrame(columns=result_columns)

        data = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
        for group, group_resources in self._groupby(resources):

            def _is_group(row: pd.Series) -> bool:
                return all(row[col] == val for col, val in group.items())

            group_columns = self.__get_columns(group_resources)
            group_data = data[data.apply(_is_group, axis="columns")]

            for column in [c for c in group_columns if isinstance(c, DatetimeColumn)]:
                group_data[column.name] = group_data[column.name].dt.tz_localize(column.timezone)

            if self.datetime_index_type == DatetimeIndexType.DATE_AND_TIME:
                date_column, time_column, *_ = self.primary_key.columns
                index_column = date_column.name + time_column.name
                index_data = pd.to_datetime(group_data[date_column.name]) + group_data[time_column.name]
                group_data.loc[:, [index_column]] = index_data
                group_data.drop([date_column.name, time_column.name], inplace=True, axis="columns")
                group_data = group_data.set_index(index_column)  # .tz_localize(tz.UTC)

            elif self.datetime_index_type in (DatetimeIndexType.TIMESTAMP, DatetimeIndexType.DATETIME):
                index_column, *_ = self.primary_key.columns
                group_data = group_data.set_index(index_column.name)

            elif self.datetime_index_type == DatetimeIndexType.TIMESTAMP_UNIX:
                index_column, *_ = self.primary_key.columns
                index_data = pd.to_datetime(group_data[index_column.name], unit="s")
                group_data.drop(index_column.name, inplace=True, axis="columns")
                group_data.loc[:, [index_column.name]] = index_data
                group_data = group_data.set_index(index_column.name).tz_localize(tz.UTC)

            group_data = group_data.dropna(axis="index", how="all").rename(
                columns={r.get("column", default=r.key): r.id for r in group_resources}
            )
            results.append(group_data[group_resources.ids])

        results = pd.concat(sorted(results, key=lambda d: min(d.index)), axis="index")
        for result_column in [c for c in result_columns if c not in results.columns]:
            results.loc[:, [result_column]] = np.nan
        return results

    def exists(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> Select:
        columns = self.__get_columns(resources)
        query = sql.select(*columns).where(and_(*self._primary_clauses(start, end))).subquery()
        query = sql.select(func.count().label("count")).select_from(query)
        return query

    # noinspection PyShadowingBuiltins, PyProtectedMember, PyArgumentList
    def hash(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
        method: Literal["MD5", "SHA1", "SHA256", "SHA512"] = "MD5",
    ) -> Select:
        if method.lower() not in ["md5", "sha1", "sha256", "sha512"]:
            raise ValueError(f"Invalid checksum method '{method}'")
        method = getattr(func, method.lower())
        columns = self.__get_columns(resources)

        def _validate(column: Column) -> ClauseElement:
            if column.type == DATETIME or isinstance(column.type, DATETIME):
                # TODO: Verify if there is a more generic way to implement time
                raise ValueError(
                    f"Unable to generate consistent hashes for table '{self.name}' "
                    f"with DATETIME column: {column.name}",
                )
            if column.type == TIMESTAMP or isinstance(column.type, TIMESTAMP):
                return func.unix_timestamp(column)
            if column.type == BLOB or isinstance(column.type, BLOB):
                return method(column)
            else:
                return column

        select = sql.select(*[_validate(c) for c in columns])
        nullable = [c.name for c in columns if c.nullable]
        if len(nullable) > 0:
            # Make sure to only query valid values
            select = select.filter(not_(and_(*[c.is_(None) for c in columns if c.nullable])))
        select = select.where(and_(*self._primary_clauses(start, end)))
        select = select.order_by(*self._primary_order("asc"))
        select = select.subquery(name="hash_range")

        concat = func.aggregate_strings(func.concat_ws(",", *select.exported_columns.values()), ",")

        query = sql.select(method(concat).label("hash"), literal(True).label("in_range")).group_by(text("in_range"))
        return query.select_from(select)

    def read(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
        order_by: Literal["asc", "desc"] = "asc",
    ) -> Select:
        columns = self.__get_columns(resources)
        query = sql.select(*columns)
        query = query.where(and_(*self._primary_clauses(start, end)))
        return query.order_by(*self._primary_order(order_by))

    # noinspection PyUnresolvedReferences
    def write(self, resources: Resources, data: pd.DataFrame) -> Insert:
        resources = resources.filter(lambda r: r.id in data.columns)
        resource_columns = self.__get_resource_columns(resources)
        primary_columns = self.primary_key.columns
        params = self._validate(resources, data.replace(np.nan, None))

        # Handle duplicate primary keys (upsert)
        if self.dialect.name == "postgresql":
            from sqlalchemy.dialects import postgresql

            query = postgresql.insert(self).values(params)
            return query.on_conflict_do_update(
                index_elements=[c.name for c in primary_columns],
                set_={c.name: c for c in resource_columns},
            )
        elif self.dialect.name in ["mariadb", "mysql"]:
            from sqlalchemy.dialects import mysql

            query = mysql.insert(self).values(params)
            return query.on_duplicate_key_update({c.name: c for c in resource_columns})
        else:
            return sql.insert(self).values(params)

    # noinspection PyMethodOverriding
    def delete(
        self,
        resources: Resources,
        start: Optional[TimestampType] = None,
        end: Optional[TimestampType] = None,
    ) -> Delete:
        columns = self.__get_columns(resources)
        if self.columns != columns:
            raise ResourceException(f"Unable to delete rows of table '{self.name}' with only subset of columns")
        query = super().delete()
        query = query.where(and_(*self._primary_clauses(start, end)))
        return query

    def _validate(self, resources: Resources, data: pd.DataFrame) -> List[Dict[str, Any]]:
        values = []

        for group, group_resources in self._groupby(resources):
            group_columns = self.__get_columns(group_resources)
            group_data = data[group_resources.ids].dropna(axis="index", how="all")

            array_resources = group_resources.filter(lambda r: issubclass(r.type, pd.api.extensions.ExtensionArray))
            if len(array_resources) > 0:
                group_data = group_data.explode([r.id for r in array_resources])
            group_data.rename(columns={r.id: r.get("column", default=r.key) for r in group_resources}, inplace=True)

            for column in group_columns:
                if self.__is_datetime_index(column):
                    column_data = group_data.index
                elif column.name in group_data.columns:
                    column_data = group_data[column.name]
                elif column.name in group:
                    column_data = group[column.name]
                else:
                    continue
                group_data[column.name] = column.validate(column_data)

            values.extend(group_data.to_dict(orient="records"))
        return values

    # noinspection SpellCheckingInspection
    def _groupby(self, resources: Resources) -> Iterator[Tuple[Dict[str, Any], Resources]]:
        groups: List[Tuple[Dict[str, Any], Resources]] = []

        def _group(resource: Resource) -> Resource:
            attributes = {}
            surrogate_keys = [c for c in self.primary_key.columns if isinstance(c, SurrogateKeyColumn)]
            for column in surrogate_keys:
                if not hasattr(resource, column.attribute):
                    raise ResourceException(
                        f"SQL resource '{resource.id}' missing surrogate key attribute: {column.attribute}"
                    )
                attributes[column.name] = getattr(resource, column.attribute)
            for group in groups:
                if group[0] == attributes:
                    group[1].append(resource)
                    return resource
            groups.append((attributes, Resources([resource])))
            return resource

        resources.apply(_group)
        return iter(groups)

    def __get_columns(self, resources: Resources) -> List[Column]:
        return [*self.primary_key.columns, *self.__get_resource_columns(resources)]

    def __get_resource_columns(self, resources: Resources) -> List[Column]:
        columns = []
        for resource in resources:
            column_name = resource.get("column", default=resource.key)
            if column_name in self.columns:
                column = self.columns[column_name]
                if column not in columns + self.primary_key.columns.values():
                    columns.append(column)
        return columns

    # noinspection PyShadowingBuiltins, PyProtectedMember
    def __get_datetime_index(self, type: DatetimeIndexType) -> List[Column]:
        # Datetime index column should be the first column(s), otherwise they will be ignored
        columns = []
        for i in range(len(type.value)):
            if i >= len(self.primary_key.columns):
                break
            column = self.primary_key.columns[i]
            if column.type in type:
                columns.append(column)
        return columns

    # noinspection PyShadowingBuiltins
    def __has_datetime_index(self, *types: DatetimeIndexType) -> bool:
        return any(len(self.__get_datetime_index(t)) == len(t.value) for t in types)

    # noinspection PyShadowingBuiltins
    def __is_datetime_index(self, column: Column) -> bool:
        if self.datetime_index_type == DatetimeIndexType.NONE:
            return False
        return column in self.__get_datetime_index(self.datetime_index_type)
