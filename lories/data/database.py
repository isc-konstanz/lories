# -*- coding: utf-8 -*-
"""
lories.data.database
~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
from abc import abstractmethod
from functools import wraps
from typing import Any, Optional, overload

import tzlocal

import pandas as pd
import pytz as tz
from lories._core._database import _Database  # noqa
from lories.connectors.connector import Connector, ConnectorMeta
from lories.connectors.errors import ConnectionError, ConnectorError
from lories.core import Configurations, Resources
from lories.core.typing import Timestamp
from lories.data.util import hash_data
from lories.data.validation import validate_index, validate_timezone
from lories.util import convert_timezone, to_date, to_timezone

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class DatabaseMeta(ConnectorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        database = super().__call__(*args, **kwargs)
        cls._wrap_method(database, "hash")
        cls._wrap_method(database, "exists")
        cls._wrap_method(database, "read_first")
        cls._wrap_method(database, "read_first_index")
        cls._wrap_method(database, "read_last")
        cls._wrap_method(database, "read_last_index")
        cls._wrap_method(database, "delete")

        return database


# noinspection PyUnresolvedReferences
class Database(_Database, Connector, metaclass=DatabaseMeta):
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
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        method: Literal["MD5", "SHA1", "SHA256", "SHA512"] = "MD5",
        encoding: str = "UTF-8",
    ) -> Optional[str]:
        data = self._run_read(resources, start, end)
        data = self._validate(resources, data)
        data = self._get_range(data, start, end)
        if data is None or data.empty:
            return None

        columns = [r.id for r in resources if r.id in data.columns]
        return hash_data(data.loc[:, columns], method, encoding)

    @wraps(hash, updated=())
    def _do_hash(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        method: Literal["MD5", "SHA1", "SHA256", "SHA512"] = "MD5",
        encoding: str = "UTF-8",
        locking: bool = True,
        *args,
        **kwargs,
    ) -> Optional[str]:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(
                    self, f"Timeout acquiring lock for disconnecting " f"{type(self).__name__}: {self.id}"
                )
            if not self._is_connected():
                raise ConnectionError(self, f"Database '{self.id}' not connected")

            return self._run_hash(resources, start=start, end=end, method=method, encoding=encoding, *args, **kwargs)

        finally:
            self._lock.release()

    def exists(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> bool:
        # TODO: Replace this placeholder more resource efficient
        data = self._run_read(resources, start, end)
        data = self._validate(resources, data)
        data = self._get_range(data, start, end)
        return not data.empty

    @wraps(exists, updated=())
    def _do_exists(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        locking: bool = True,
        *args,
        **kwargs,
    ) -> bool:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(self, f"Timeout acquiring lock for disconnecting {type(self).__name__}: {self.id}")
            if not self._is_connected():
                raise ConnectionError(self, f"Database '{self.id}' not connected")

            return self._run_exists(resources, start=start, end=end, *args, **kwargs)

        finally:
            self._lock.release()

    @overload
    def read(self, resources: Resources) -> pd.DataFrame: ...

    @overload
    def read(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def read(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame: ...

    @wraps(read, updated=())
    def _do_read(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        locking: bool = True,
        *args,
        **kwargs,
    ) -> pd.DataFrame:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(self, f"Timeout acquiring lock for disconnecting {type(self).__name__}: {self.id}")
            if not self._is_connected():
                raise ConnectionError(self, f"Trying to read from unconnected {type(self).__name__}: {self.id}")

            data = self._run_read(resources, start=start, end=end, *args, **kwargs)
            data = self._validate(resources, data)
            return self._get_range(data, start, end)

        finally:
            self._lock.release()

    @abstractmethod
    def read_first(self, resources: Resources) -> Optional[pd.DataFrame]: ...

    @wraps(read_first, updated=())
    def _do_read_first(self, resources: Resources, locking: bool = True, *args, **kwargs) -> Optional[pd.DataFrame]:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(
                    self,
                    f"Timeout acquiring lock for reading first values " f"of {type(self).__name__}: {self.id}",
                )
            if not self._is_connected():
                raise ConnectionError(self, f"Database '{self.id}' not connected")

            data = self._run_read_first(resources, *args, **kwargs)
            data = self._validate(resources, data)
            return data

        finally:
            self._lock.release()

    def read_first_index(self, resources: Resources) -> Optional[Any]:
        data = self._run_read_first(resources)
        data = self._validate(resources, data)
        if data is None or data.empty:
            return None
        return min(data.index)

    @wraps(read_first_index, updated=())
    def _do_read_first_index(self, resources: Resources, locking: bool = True, *args, **kwargs) -> Optional[Any]:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(
                    self,
                    f"Timeout acquiring lock for reading first index " f"of {type(self).__name__}: {self.id}",
                )
            if not self._is_connected():
                raise ConnectionError(self, f"Database '{self.id}' not connected")

            index = self._run_read_first_index(resources, *args, **kwargs)
            if isinstance(index, (pd.Timestamp, dt.datetime)):
                index = convert_timezone(index, timezone=self.timezone)
            return index

        finally:
            self._lock.release()

    @abstractmethod
    def read_last(self, resources: Resources) -> Optional[pd.DataFrame]: ...

    @wraps(read_last, updated=())
    def _do_read_last(self, resources: Resources, locking: bool = True, *args, **kwargs) -> Optional[pd.DataFrame]:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(
                    self,
                    f"Timeout acquiring lock for reading last values " f"of {type(self).__name__}: {self.id}",
                )
            if not self._is_connected():
                raise ConnectionError(self, f"Database '{self.id}' not connected")

            data = self._run_read_last(resources, *args, **kwargs)
            data = self._validate(resources, data)
            return data

        finally:
            self._lock.release()

    def read_last_index(self, resources: Resources) -> Optional[pd.Index]:
        data = self._run_read_last(resources)
        data = self._validate(resources, data)
        if data is None or data.empty:
            return None
        return max(data.index)

    @wraps(read_last_index, updated=())
    def _do_read_last_index(self, resources: Resources, locking: bool = True, *args, **kwargs) -> Optional[Any]:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(
                    self,
                    f"Timeout acquiring lock for reading last index " f"of {type(self).__name__}: {self.id}",
                )
            if not self._is_connected():
                raise ConnectionError(self, f"Database '{self.id}' not connected")

            index = self._run_read_last_index(resources, *args, **kwargs)
            if isinstance(index, (pd.Timestamp, dt.datetime)):
                index = convert_timezone(index, timezone=self.timezone)
            return index

        finally:
            self._lock.release()

    def _validate(self, resources: Resources, data: pd.DataFrame) -> pd.DataFrame:
        if not data.empty:
            data = validate_index(data)
            data.index = validate_timezone(data.index, self.timezone)
            for resource in resources:
                if resource.id not in data:
                    continue
                if resource.type in [pd.Timestamp, dt.datetime]:
                    resource_data = data[resource.id]
                    if pd.api.types.is_string_dtype(resource_data.values):
                        resource_data = pd.to_datetime(resource_data)
                    data[resource.id] = validate_timezone(resource_data, self.timezone)
        return data

    @staticmethod
    def _get_range(
        data: pd.DataFrame,
        start: Optional[Timestamp | str] = None,
        end: Optional[Timestamp | str] = None,
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

    @overload
    def delete(self, resources: Resources) -> None: ...

    @overload
    def delete(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> None: ...

    def delete(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> None:
        raise NotImplementedError(f"Unable to delete values for database '{self.id}'")

    @wraps(delete, updated=())
    def _do_delete(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        locking: bool = True,
        *args,
        **kwargs,
    ) -> None:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(
                    self,
                    f"Timeout acquiring lock for deleting data " f"of {type(self).__name__}: {self.id}",
                )
            if not self._is_connected():
                raise ConnectionError(self, f"Database '{self.id}' not connected")

            self._run_delete(resources, start=start, end=end, *args, **kwargs)
        finally:
            self._lock.release()
