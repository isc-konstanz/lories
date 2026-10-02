# -*- coding: utf-8 -*-
"""
lories.connectors.tables
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import json
import os
import re
from typing import Optional, Sequence

import numpy as np
import pandas as pd
from lories.connectors import ConnectionError, Database, register_connector_type
from lories.core.configs.parameters import ChannelParameter, Parameter, SelectParameter
from lories.typing import Configurations, Resources, Timestamp
from lories.util import to_json_compatible
from pandas import HDFStore


@register_connector_type("tables", "hdfstore")
class HDFDatabase(Database):
    """
    HDF5 (Hierarchical Data Format version 5) is a binary file format designed for storing and organizing large
    amounts of numerical data efficiently. Using the PyTables-backed pandas HDFStore, it supports fast columnar
    reads, on-disk querying, and optional compression. However, HDF5 files are not easily human-readable,
    concurrent write access is limited, and the format can be sensitive to library version mismatches.
    """

    _path = Parameter(
        key="path",
        type=str,
        required=False,
        desc="Directory path (relative to data dir or absolute) or absolute file path for the HDF5 store",
    )
    _file = Parameter(key="file", type=str, default=".store.h5", desc="HDF5 store filename")
    _mode = SelectParameter(
        ["a", "r", "r+", "w"],
        key="mode",
        default="a",
        desc="HDF5 file open mode: a=append, r=read-only, r+=read/write existing, w=truncate",
    )
    _columns_unique = Parameter(
        key="columns_unique",
        type=bool,
        default=False,
        desc="Use channel IDs as column names instead of channel keys (avoids collisions across groups)",
    )
    _compression_level = Parameter(
        key="compression_level",
        type=int,
        required=False,
        min=0,
        max=9,
        desc="PyTables compression level (0=off, 9=maximum)",
    )
    _compression_lib = Parameter(
        key="compression_lib",
        type=str,
        required=False,
        choices=["zlib", "lzo", "bzip2", "blosc"],
        desc="PyTables compression library",
    )

    # Per-channel parameters
    group = ChannelParameter(type=str, required=False, desc="HDF5 group key under which this channel is stored")

    __store: HDFStore = None

    _store_dir: str
    _store_path: str

    _mode: str
    _columns_unique: bool
    _compression_level: int | None
    _compression_lib: str | None

    # noinspection PyTypeChecker
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        if self._path is not None:
            path = self._path
            if "~" in path:
                path = os.path.expanduser(path)
            if os.path.isabs(path):
                store_dir = os.path.dirname(path)
                store_path = path
            else:
                store_dir = os.path.join(configs.dirs.data, path)
                store_path = os.path.join(store_dir, self._file)
        else:
            store_dir = configs.dirs.data
            store_path = os.path.join(store_dir, self._file)
        self._store_dir = store_dir
        self._store_path = store_path

    def is_connected(self) -> bool:
        return self.__store is not None and self.__store.is_open

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        if not os.path.isdir(self._store_dir):
            os.makedirs(self._store_dir, exist_ok=True)
        try:
            self.__store = HDFStore(
                self._store_path,
                mode=self._mode,
                complevel=self._compression_level,
                complib=self._compression_lib,
            )
            self.__store.open()

        except IOError as e:
            raise ConnectionError(self, str(e))

    def disconnect(self) -> None:
        super().disconnect()
        if self.__store is not None:
            self.__store.close()

    # noinspection PyTypeChecker, PyUnresolvedReferences
    def read(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame:
        data = []
        try:
            for group, group_resources in resources.groupby("group"):
                group_key = _format_key(group)
                if group_key not in self.__store:
                    continue

                group_columns = self.__build_columns(group_resources)
                group_data = self.__store.select(
                    group_key,
                    where=_build_where(start, end),
                    columns=group_columns,
                )
                group_data = self.__extract_data(group_resources, group_data)
                if not group_data.empty:
                    data.append(group_data)
        except IOError as e:
            raise ConnectionError(self, str(e))

        if len(data) == 0:
            return pd.DataFrame()
        data = sorted(data, key=lambda d: min(d.index))
        return pd.concat(data, axis="columns")

    # noinspection PyTypeChecker, PyUnresolvedReferences
    def read_first(self, resources: Resources) -> Optional[pd.DataFrame]:
        data = []
        try:
            for group, group_resources in resources.groupby("group"):
                group_key = _format_key(group)
                if group_key not in self.__store:
                    continue

                group_data = self.__store.select(group_key, stop=1, columns=self.__build_columns(group_resources))
                group_data = self.__extract_data(group_resources, group_data)
                if not group_data.empty:
                    data.append(group_data)
        except IOError as e:
            raise ConnectionError(self, str(e))

        if len(data) == 0:
            return pd.DataFrame()
        data = sorted(data, key=lambda d: min(d.index))
        return pd.concat(data, axis="columns")

    # noinspection PyTypeChecker, PyUnresolvedReferences
    def read_last(self, resources: Resources) -> Optional[pd.DataFrame]:
        data = []
        try:
            for group, group_resources in resources.groupby("group"):
                group_key = _format_key(group)
                if group_key not in self.__store:
                    continue

                group_data = self.__store.select(group_key, start=-1, columns=self.__build_columns(group_resources))
                group_data = self.__extract_data(group_resources, group_data)
                if not group_data.empty:
                    data.append(group_data)
        except IOError as e:
            raise ConnectionError(self, str(e))

        if len(data) == 0:
            return pd.DataFrame()
        data = sorted(data, key=lambda d: min(d.index))
        return pd.concat(data, axis="columns")

    def delete(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> None:
        try:
            for group, group_resources in resources.groupby("group"):
                group_key = _format_key(group)
                if group_key not in self.__store:
                    continue

                self.__store.remove(group_key)

        except IOError as e:
            raise ConnectionError(self, str(e))

    def write(self, data: pd.DataFrame) -> None:
        try:
            for group, group_resources in self.resources.filter(lambda c: c.id in data.columns).groupby("group"):
                group_key = _format_key(group)
                group_data = data[group_resources.ids].dropna(axis="index", how="all").dropna(axis="columns", how="all")
                group_data.index.name = "index"

                if not self._columns_unique:
                    group_data.rename(
                        columns={r.id: r.get("column", default=r.key) for r in group_resources},
                        inplace=True,
                    )
                widths = {}
                for resource in group_resources:
                    if not issubclass(resource.type, list):
                        continue
                    name = resource.id if self._columns_unique else resource.get("column", default=resource.key)
                    if name not in group_data.columns:
                        continue
                    group_data[name], widths[name] = _encode_lists(group_data[name])
                if group_key not in self.__store:
                    self.__store.put(
                        group_key,
                        group_data,
                        format="table",
                        encoding="UTF-8",
                        min_itemsize=widths or None,
                    )
                else:
                    self.__store.append(group_key, group_data, format="table", encoding="UTF-8")

        except IOError as e:
            raise ConnectionError(self, str(e))

    def __build_columns(self, resources: Resources) -> Sequence[str]:
        if self._columns_unique:
            return [r.id for r in resources]
        return [r.get("column", default=r.key) for r in resources]

    def __extract_data(self, resources: Resources, data: pd.DataFrame) -> pd.DataFrame:
        data.dropna(axis="columns", how="all", inplace=True)
        if not self._columns_unique:
            data = data.rename(columns={r.get("column", default=r.key): r.id for r in resources})
        for resource in resources:
            if issubclass(resource.type, list) and resource.id in data.columns:
                data[resource.id] = _decode_lists(data[resource.id])
        return data


def _is_missing(value) -> bool:
    if isinstance(value, (list, tuple, np.ndarray)):
        return False
    return bool(pd.isna(value))


def _encode_lists(column: pd.Series) -> tuple[pd.Series, int]:
    width = 1
    cells = []
    for value in column:
        if _is_missing(value):
            cells.append(np.nan)
            continue
        cell = json.dumps(to_json_compatible(value), separators=(",", ":"), allow_nan=False)
        width = max(width, len(cell), 25 * len(value) + 1)
        cells.append(cell)
    return pd.Series(cells, index=column.index, dtype=object, name=column.name), width


def _decode_lists(column: pd.Series) -> pd.Series:
    cells = [json.loads(v) if isinstance(v, str) else np.nan for v in column]
    return pd.Series(cells, index=column.index, dtype=object, name=column.name)


def _format_key(key: str) -> str:
    key = re.sub(r"\W", "/", key).replace("_", "/").lower()
    return f"/{key}"


def _build_where(
    start: Optional[Timestamp] = None,
    end: Optional[Timestamp] = None,
) -> Optional[str]:
    where = []
    if start is not None:
        where.append(f'index>="{start.isoformat()}"')
    if end is not None:
        where.append(f'index<="{end.isoformat()}"')
    return " & ".join(where) if len(where) > 0 else None
