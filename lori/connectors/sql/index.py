# -*- coding: utf-8 -*-
"""
lori.connectors.sql.index
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from enum import Enum
from typing import Any, AnyStr, Dict, Optional, Sequence, Tuple, Type

import numpy as np
import pandas as pd
import pytz as tz
from lori.connectors.sql.column import Column, Columns
from lori.core import Configurations, Resource, ResourceException, Resources


class DatetimeIndexType(Enum):
    DATE_AND_TIME = ("DATE", "TIME")
    DATETIME = ("DATETIME",)
    TIMESTAMP = ("TIMESTAMP",)
    TIMESTAMP_UNIX = ("INT",)


# noinspection PyShadowingBuiltins
class IndexColumn(Column):
    DEFAULT_NAME: str = "timestamp"
    DEFAULT_TYPE: str = "TIMESTAMP"

    @classmethod
    def from_defaults(cls) -> IndexColumn:
        return cls(cls.DEFAULT_NAME, cls.DEFAULT_TYPE)

    def __init__(
        self,
        name: str,
        type: AnyStr | Type | int,
        nullable: bool = False,
        datetime: Optional[bool] = None,
        **kwargs,
    ) -> None:
        super().__init__(name, type, nullable=nullable, **kwargs)
        if datetime is None:
            datetime = _is_datetime(self.type)
        self._datetime = datetime

        # TODO: Implement default assertions for more primary columns, like date and time
        if self.default is None:
            if self.type == "INT" and datetime:
                self.default = "(UNIX_TIMESTAMP())"
            elif self.type in ["DATETIME", "TIMESTAMP"]:
                self.default = "CURRENT_TIMESTAMP"

    # @property
    # def placeholder(self) -> str:
    #     placeholder = super().placeholder
    #     if self.type == "TIMESTAMP":
    #         return f"FROM_UNIXTIME({placeholder})"
    #     return placeholder

    def is_datetime(self) -> bool:
        return self._datetime

    # noinspection PyUnresolvedReferences
    def prepare(self, data: Any, timezone: tz.BaseTzInfo) -> Any:
        if self._datetime:
            if (
                data is None
                or not isinstance(data, (pd.Series, pd.DatetimeIndex, dt.datetime))
                or (isinstance(data, pd.Series) and not data.map(lambda i: isinstance(i, dt.datetime)).all())
            ):
                raise ResourceException(f"Unable to prepare datetime index from '{type(data)}' data: {data}")

            if self.type in ["TIMESTAMP", "INT"]:
                if isinstance(data, pd.Series):
                    data = data.dt.tz_convert(tz.UTC)
                elif isinstance(data, (pd.DatetimeIndex, pd.Timestamp)):
                    data = data.tz_convert(tz.UTC)
                else:
                    data = data.astimezone(tz.UTC)

                if self.type == "INT":
                    if isinstance(data, (pd.DatetimeIndex, pd.Series)):
                        return data.astype(np.int64) // 10**9
                    elif isinstance(data, pd.Timestamp):
                        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
                        return int((data - epoch).total_seconds())
                    else:
                        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
                        return int((data - epoch).total_seconds())

            if self.type in ["DATETIME", "DATE", "TIME"]:
                if isinstance(data, pd.Series):
                    data = data.dt.tz_convert(timezone)
                elif isinstance(data, (pd.DatetimeIndex, pd.Timestamp)):
                    data = data.tz_convert(timezone)
                else:
                    data = data.astimezone(timezone)

                if self.type == "DATE":
                    if isinstance(data, (pd.DatetimeIndex, pd.Series)):
                        return data.date
                    else:
                        return data.date()

                if self.type == "TIME":
                    if isinstance(data, (pd.DatetimeIndex, pd.Series)):
                        return data.time
                    else:
                        return data.time()

        if data is None and not self.nullable:
            raise ResourceException(f"None value for '{self.name}' NOT NULL")
        return data


# noinspection PyShadowingBuiltins, SpellCheckingInspection
class Index(Columns[IndexColumn]):
    @classmethod
    def from_configs(cls, configs: Configurations, **kwargs) -> Index:
        if "type" in configs and configs.get("type") not in [None, "None"]:
            index_type = DatetimeIndexType[configs.get("type").upper()]
            index = cls.from_type(index_type, configs.get("column", default=None), **kwargs)
        else:
            index = cls(IndexColumn.from_defaults(), **kwargs)
        return index

    @classmethod
    def from_type(cls, type: DatetimeIndexType, name: Optional[str] = None, **kwargs) -> Index:
        columns = cls(**kwargs)
        for column_type in type.value:
            column_name = name if name is not None else column_type.lower()
            columns.add(column_name, column_type, datetime=True)
        return columns

    def __init__(self, *columns: IndexColumn, timezone: tz.BaseTzInfo = tz.UTC) -> None:
        super().__init__(*columns)
        self.timezone = timezone

    # noinspection PyShadowingBuiltins
    def add(
        self,
        name: str,
        type: AnyStr | Type | int,
        attribute: Optional[str] = None,
        length: Optional[int] = None,
        default: Optional[Any] = None,
        **kwargs,
    ) -> None:
        if attribute is None or len(attribute) == 0:
            self.append(IndexColumn(name, type, length=length, default=default, **kwargs))
        else:
            self.append(SurrogateKeyColumn(name, type, attribute, length=length, default=default, **kwargs))

    def process(self, resources: Resources, data: pd.DataFrame) -> pd.DataFrame:
        result_columns = [r.id for r in resources]
        results = []
        if data.empty:
            return pd.DataFrame(columns=result_columns)

        for group, group_resources in self._groupby(resources):

            def _is_group(row: pd.Series) -> bool:
                return all(row[col] == val for col, val in group.items())

            group_data = data[data.apply(_is_group, axis="columns")]

            for index in self:
                # TODO: Validate TIMESTAMP timezone handling
                # if index.type == "TIMESTAMP":
                #     group_data[index.name] = group_data[index.name].dt.tz_localize(tz.UTC).dt.tz_convert(self.timezone)
                if index.type in ["TIMESTAMP", "DATETIME"]:
                    group_data[index.name] = group_data[index.name].dt.tz_localize(self.timezone, ambiguous="infer")

            if self._has_datetime(DatetimeIndexType.DATE_AND_TIME):
                date_column, time_column, *_ = self.names
                index_column = date_column + time_column
                group_data.loc[:, [index_column]] = pd.to_datetime(group_data[date_column]) + group_data[time_column]
                group_data.drop([date_column, time_column], inplace=True, axis="columns")
                group_data = group_data.set_index(index_column).tz_localize(self.timezone, ambiguous="infer")

            elif self._has_datetime(DatetimeIndexType.TIMESTAMP) or self._has_datetime(DatetimeIndexType.DATETIME):
                index_column, *_ = self.names
                group_data = group_data.set_index(index_column)

            elif self._has_datetime(DatetimeIndexType.TIMESTAMP_UNIX):
                index_column, *_ = self.names
                group_data.loc[:, [index_column]] = pd.to_datetime(group_data[index_column], unit="s")
                group_data = group_data.set_index(index_column).tz_localize(tz.UTC).tz_convert(self.timezone)

            group_columns = {r.column if "column" in r else r.key: r.id for r in group_resources}
            group_data = group_data[group_columns.keys()].dropna(axis="index", how="all").rename(columns=group_columns)
            results.append(group_data)

        results = pd.concat(results, axis="index")
        for result_column in [c for c in result_columns if c not in results.columns]:
            results.loc[:, [result_column]] = np.nan
        return results

    def prepare(self, resources: Resources, data: pd.DataFrame) -> Iterator[pd.DataFrame]:
        for group, group_resources in self._groupby(resources):
            group_columns = {r.id: r.column if "column" in r else r.key for r in group_resources}
            group_data = data[group_columns.keys()].dropna(axis="index", how="all").rename(columns=group_columns)
            for group_column, group_value in group.items():
                group_data[group_column] = group_value
            for index in self:
                if self._is_datetime(index) or index.name not in group_data.columns:
                    index_data = group_data.index
                else:
                    index_data = group_data[index.name]
                group_data[index.name] = index.prepare(index_data, self.timezone)
            yield group_data

    # noinspection PyProtectedMember
    def _get_datetime(self, type: DatetimeIndexType) -> Sequence[IndexColumn]:
        # Datetime index column should be the first column(s), otherwise they will be ignored
        columns = []
        for i in range(len(type.value)):
            if i >= len(self):
                break
            column = self[i]
            if column.is_datetime() and column.type in type.value:
                columns.append(column)
        return columns

    def _has_datetime(self, type: DatetimeIndexType) -> bool:
        return len(self._get_datetime(type)) == len(type.value)

    def _is_datetime(self, column: IndexColumn) -> bool:
        return column.is_datetime() and any(column in self._get_datetime(t) for t in DatetimeIndexType)

    def _get_surrogate_keys(self) -> Sequence[SurrogateKeyColumn]:
        return [c for c in self if self._is_surrogate_key(c)]

    def _has_surrogate_keys(self) -> bool:
        return len(self._get_surrogate_keys()) > 0

    # noinspection PyMethodMayBeStatic
    def _is_surrogate_key(self, column: IndexColumn) -> bool:
        return isinstance(column, SurrogateKeyColumn)

    def _groupby(self, resources: Resources) -> Iterator[Tuple[Dict[str, Any], Resources]]:
        groups = list[Tuple[Dict[str, Any], Resources]]()

        def _group(resource: Resource) -> Resource:
            attributes = {}
            for column in self._get_surrogate_keys():
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

    def where(
        self,
        query: str,
        start: pd.Timestamp | dt.datetime = None,
        end: pd.Timestamp | dt.datetime = None,
    ) -> Tuple[str, Dict[str, Any]]:
        params = {}
        where = []

        # TODO: Implement start and end type checking and allow e.g. date and time columns or integers as well
        index = self.first()
        if index.is_datetime():
            if start is not None:
                start_key = f"{index.name}_start"
                where.append(f"`{index.name}` >= {index.placeholder.replace(index.name, start_key)}")
                params[start_key] = index.prepare(start, self.timezone)
            if end is not None:
                end_key = f"{index.name}_end"
                where.append(f"`{index.name}` <= {index.placeholder.replace(index.name, end_key)}")
                params[end_key] = index.prepare(end, self.timezone)
        if len(where) > 0:
            query = f"{query} WHERE {' AND '.join(where)}"
        return query, params

    def order_by(self, order: str = "ASC") -> str:
        return "ORDER BY " + ", ".join(f"`{i.name}` {order}" for i in reversed(self) if not self._is_surrogate_key(i))


# noinspection PyShadowingBuiltins
class SurrogateKeyColumn(IndexColumn):
    def __init__(
        self,
        name: str,
        type: AnyStr | Type | int,
        attribute: str,
        **kwargs,
    ) -> None:
        super().__init__(name, type, **kwargs)
        self.attribute = attribute


# noinspection PyShadowingBuiltins
def _is_datetime(type: str) -> bool:
    return type.upper() in [
        "TIME",
        "DATE",
        "DATETIME",
        "TIMESTAMP",
    ]
