# -*- coding: utf-8 -*-
"""
lori.connectors.tables
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import os
import re
from typing import Optional, Sequence

import pandas as pd
from lori.connectors import ConnectionError, Database, register_connector_type
from lori.typing import Configurations, Resources, Timestamp
from pandas import HDFStore


@register_connector_type("tables", "hdfstore")
class HDFDatabase(Database):
    __store: HDFStore = None

    _store_dir: str
    _store_path: str

    _mode: str = "a"

    _columns_unique: bool = True

    _compression_level: int | None = None
    _compression_lib = None

    # noinspection PyTypeChecker
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        store_dir = configs.get("path", default=None)
        if store_dir is not None:
            if "~" in store_dir:
                store_dir = os.path.expanduser(store_dir)
            if not os.path.isabs(store_dir):
                store_dir = os.path.join(configs.dirs.data, store_dir)
        else:
            store_dir = configs.dirs.data

        store_path = configs.get("path", default=None)
        if store_path is not None:
            if not os.path.isabs(store_path):
                store_path = os.path.join(store_dir, store_path)
            else:
                store_dir = os.path.dirname(store_path)
        else:
            store_file = configs.get("file", default=".store.h5")
            store_path = os.path.join(store_dir, store_file)
        self._store_dir = store_dir
        self._store_path = store_path

        self._mode = configs.get("mode", default="a")
        self._columns_unique = configs.get_bool("columns_unique", default=False)
        self._compression_level = configs.get_int("compression_level", None)
        self._compression_lib = configs.get("compression_lib", None)

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

                group_data = self.__store.select(
                    group_key,
                    where=_build_where(start, end),
                    columns=self.__build_columns(group_resources),
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

                if not self._columns_unique:
                    group_data.rename(
                        columns={r.id: r.get("column", default=r.key) for r in group_resources},
                        inplace=True,
                    )
                if group_key not in self.__store:
                    self.__store.put(group_key, group_data, format="table", encoding="UTF-8")
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
            return data.rename(columns={r.get("column", default=r.key): r.id for r in resources})
        return data


def _format_key(key: str) -> str:
    key = re.sub(r"\W", "/", key).replace("_", "/").lower()
    return f"/{key}"


def _build_where(
    start: Optional[Timestamp] = None,
    end: Optional[Timestamp] = None,
) -> Optional[str]:
    where = []
    if start is not None:
        where.append(f'index>=Timestamp("{start.isoformat()}")')
    if end is not None:
        where.append(f'index<=Timestamp("{end.isoformat()}")')
    return " & ".join(where) if len(where) > 0 else None
