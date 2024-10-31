# -*- coding: utf-8 -*-
"""
loris.data.database
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import hashlib
from abc import abstractmethod
from functools import wraps
from typing import Any, Optional, overload

import tzlocal

import numpy as np
import pandas as pd
import pytz as tz
from loris.connectors import (
    ConnectionException,
    Connector,
    ConnectorException,
    ConnectorMeta,
    ConnectorUnavailableException,
)
from loris.core import Configurations, Resources
from loris.util import convert_timezone, to_date, to_timezone


class DatabaseMeta(ConnectorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        database = super().__call__(*args, **kwargs)

        database._Database__read = database.read
        database.read = database._do_read

        database._Database__read_first = database.read_first
        database.read_first = database._do_read_first

        database._Database__read_first_index = database.read_first_index
        database.read_first_index = database._do_read_first_index

        database._Database__read_last = database.read_last
        database.read_last = database._do_read_last

        database._Database__read_last_index = database.read_last_index
        database.read_last_index = database._do_read_last_index

        return database


class Database(Connector, metaclass=DatabaseMeta):
    timezone: tz.BaseTzInfo

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        timezone = configs.get("timezone", None)
        if timezone is None:
            timezone = tzlocal.get_localzone_name()
        self.timezone = to_timezone(timezone)

    # noinspection PyShadowingBuiltins
    def hash(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
        method: str = "MD5",
        encoding: str = "UTF-8",
    ) -> Optional[str]:
        data = self.read(resources, start, end)
        if data is None or data.empty:
            return None
        columns = [
            data.index.name,
            *[r.id for r in resources if r.id in data.columns],
        ]
        data[data.index.name] = data.index.tz_convert(tz.UTC).astype(np.int64) // 10**9

        for column in data.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            data[column] = data[column].dt.tz_convert(tz.UTC).astype(np.int64) // 10**9

        csv = data[columns].to_csv(index=False, header=False, sep=",", decimal=".", float_format="%.10g")
        csv = ",".join(csv.replace(",,", ",").splitlines())
        if method.lower() == "md5":
            hash = hashlib.md5(csv.encode(encoding)).hexdigest()
        # elif method.lower() == "sha256":
        #     hash = hashlib.sha256(bytes).hexdigest()
        else:
            raise ValueError(f"Invalid checksum method '{method}'")
        return hash

    def exists(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> bool:
        # TODO: Replace this placeholder more resource efficient
        return not self.read(resources, start, end).empty

    @overload
    def read(self, resources: Resources) -> pd.DataFrame: ...

    @overload
    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(read, updated=())
    def _do_read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
        *args,
        **kwargs,
    ) -> pd.DataFrame:
        if not self._is_connected():
            raise ConnectionException(f"Database '{self.id}' not connected")

        data = self.__read(resources, start=start, end=end, *args, **kwargs)
        data = self._validate_timezone(resources, data)
        return self._get_range(data, start, end)

    @abstractmethod
    def read_first(self, resources: Resources) -> Optional[pd.DataFrame]:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(read_first, updated=())
    def _do_read_first(self, resources: Resources, *args, **kwargs) -> Optional[pd.DataFrame]:
        if not self._is_connected():
            raise ConnectionException(f"Database '{self.id}' not connected")

        data = self.__read_first(resources, *args, **kwargs)
        data = self._validate_timezone(resources, data)
        return data

    def read_first_index(self, resources: Resources) -> Optional[Any]:
        data = self.read_first(resources)
        if data is None or data.empty:
            return None
        return data.index[0]

    # noinspection PyUnresolvedReferences
    @wraps(read_first_index, updated=())
    def _do_read_first_index(self, resources: Resources, *args, **kwargs) -> Optional[Any]:
        if not self._is_connected():
            raise ConnectionException(f"Database '{self.id}' not connected")

        index = self.__read_first_index(resources, *args, **kwargs)
        if isinstance(index, dt.datetime):
            index = convert_timezone(index, timezone=self.timezone)
        return index

    @abstractmethod
    def read_last(self, resources: Resources) -> Optional[pd.DataFrame]:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(read_last, updated=())
    def _do_read_last(self, resources: Resources, *args, **kwargs) -> Optional[pd.DataFrame]:
        if not self._is_connected():
            raise ConnectionException(f"Database '{self.id}' not connected")

        data = self.__read_last(resources, *args, **kwargs)
        data = self._validate_timezone(resources, data)
        return data

    def read_last_index(self, resources: Resources) -> Optional[pd.Index]:
        data = self.read_last(resources)
        if data is None or data.empty:
            return None
        return data.index[-1]

    # noinspection PyUnresolvedReferences
    @wraps(read_last_index, updated=())
    def _do_read_last_index(self, resources: Resources, *args, **kwargs) -> Optional[Any]:
        if not self._is_connected():
            raise ConnectionException(f"Database '{self.id}' not connected")

        index = self.__read_last_index(resources, *args, **kwargs)
        if isinstance(index, dt.datetime):
            index = convert_timezone(index, timezone=self.timezone)
        return index

    def _validate_timezone(self, resources: Resources, data: pd.DataFrame) -> pd.DataFrame:
        # noinspection PyShadowingNames
        def _validate_series(series: pd.Series) -> pd.Series:
            if isinstance(series, pd.DatetimeIndex):
                series = series.tz_convert(self.timezone)
            elif pd.api.types.is_datetime64_dtype(series.values):
                series = series.dt.tz_convert(self.timezone)
            elif series.map(lambda i: isinstance(i, (pd.Timestamp, dt.datetime))).all():
                series = series.map(lambda i: i.astimezone(self.timezone))
            return series

        if not data.empty:
            data.index = _validate_series(data.index)
            for resource in resources:
                if resource.type in [pd.Timestamp, dt.datetime]:
                    column = resource.column if "column" in resource else resource.id
                    series = data[column]
                    if pd.api.types.is_string_dtype(series.values):
                        series = pd.to_datetime(series)
                    data[column] = _validate_series(series)
        return data

    @staticmethod
    def _get_range(
        data: pd.DataFrame,
        start: Optional[pd.Timestamp, dt.datetime, str] = None,
        end: Optional[pd.Timestamp, dt.datetime, str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        if data.empty:
            return data
        if start is not None:
            start = to_date(start, **kwargs)
            data = data[data.index >= start]
        if end is not None:
            end = to_date(end, **kwargs)
            data = data[data.index <= end]
        return data


class DatabaseException(ConnectorException):
    """
    Raise if an error occurred accessing the database.

    """


class DatabaseUnavailableException(ConnectorUnavailableException):
    """
    Raise if an accessed database can not be found.

    """
