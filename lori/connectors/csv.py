# -*- coding: utf-8 -*-
"""
lori.connectors.csv
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import os
from typing import Mapping, Optional, Tuple

import pandas as pd
from lori.connectors import ConnectionException, Database, register_connector_type
from lori.core import ConfigurationException, Configurations, Resources
from lori.io import csv
from lori.util import ceil_date, floor_date, parse_freq


# noinspection PyShadowingBuiltins
@register_connector_type("csv")
class CsvDatabase(Database):
    _data: Optional[pd.DataFrame] = None
    _data_path: Optional[str] = None
    _data_dir: str

    index_column: str = "timestamp"
    index_type: str = "timestamp"

    override: bool = False
    slice: bool = False

    freq: str = "D"
    format: str = None
    suffix: Optional[str] = None

    decimal: str = "."
    separator: str = ","

    columns: Mapping[str, str] = {}

    # noinspection PyTypeChecker
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        data_dir = configs.get("dir", default=None)
        if data_dir is not None:
            if "~" in data_dir:
                data_dir = os.path.expanduser(data_dir)
            if not os.path.isabs(data_dir):
                data_dir = os.path.join(configs.dirs.data, data_dir)

        data_path = configs.get("file", default=None)
        if data_path is not None:
            if not os.path.isabs(data_path):
                if data_dir is None:
                    data_dir = configs.dirs.data
                data_path = os.path.join(data_dir, data_path)
            elif data_dir is None:
                data_dir = os.path.dirname(data_path)

        if data_dir is None:
            data_dir = configs.dirs.data

        self._data_dir = data_dir
        self._data_path = data_path

        self.index_column = configs.get("index_column", default=CsvDatabase.index_column)
        self.index_type = configs.get("index_type", default=CsvDatabase.index_type).lower()
        if self.index_type not in ["timestamp", "unix", "none", None]:
            raise ConfigurationException(f"Unknown index type: {self.index_type}")

        self.override = configs.get_bool("override", default=CsvDatabase.override)
        self.slice = configs.get_bool("slice", default=CsvDatabase.slice)

        self.freq = parse_freq(configs.get("freq", default=CsvDatabase.freq))

        format = configs.get("format", default=CsvDatabase.format)
        if format is not None:
            self.format = format
        elif self.freq == "Y":
            self.format = "%Y"
        elif self.freq == "M":
            self.format = "%Y-%m"
        elif any([self.freq.endswith("D")]):
            self.format = "%Y%m%d"
        elif any([self.freq.endswith(s) for s in ["h", "min", "s"]]):
            self.format = "%Y%m%d_%H%M%S"
        else:
            raise ConfigurationException(f"Invalid frequency: {self.freq}")

        self.suffix = configs.get("suffix", default=CsvDatabase.suffix)
        if self.suffix is not None:
            self.format += f"_{self.suffix}"

        self.decimal = configs.get("decimal", CsvDatabase.decimal)
        self.separator = configs.get("separator", CsvDatabase.separator)

        # TODO: Implement flag if pretty printing should be used or not
        self.columns = configs.get("columns", default=CsvDatabase.columns)

    def connect(self, resources: Resources) -> None:
        if not os.path.isdir(self._data_dir):
            os.makedirs(self._data_dir, exist_ok=True)

        columns = {r.name: r.id for r in self.resources if "name" in r}
        columns.update({r.name: r.id for r in resources if "name" in r})
        columns.update({column: key for key, column in self.columns.items()})

        try:
            if self._data_path is not None:
                self._data = csv.read_file(
                    self._data_path,
                    index_column=self.index_column,
                    index_type=self.index_type,
                    timezone=self.timezone,
                    separator=self.separator,
                    decimal=self.decimal,
                    rename=columns,
                )
        except IOError as e:
            raise ConnectionException(self, str(e))

    def disconnect(self) -> None:
        self._data = None

    def is_connected(self) -> bool:
        return True

    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame:
        columns = {r.name: r.id for r in self.resources if "name" in r}
        columns.update({r.name: r.id for r in resources if "name" in r})
        columns.update({column: key for key, column in self.columns.items()})

        def _infer_dates(s=start, e=end) -> Tuple[pd.Timestamp, pd.Timestamp]:
            if all(pd.isna(d) for d in [s, e]):
                n = pd.Timestamp.now(tz=self.timezone)
                s = floor_date(n, timezone=self.timezone, freq=self.freq)
                e = ceil_date(n, timezone=self.timezone, freq=self.freq)
            return s, e

        try:
            if self._data is not None:
                data = self._data
            else:
                data = csv.read_files(
                    self._data_dir,
                    self.freq,
                    self.format,
                    *_infer_dates(),
                    index_column=self.index_column,
                    index_type=self.index_type,
                    timezone=self.timezone,
                    separator=self.separator,
                    decimal=self.decimal,
                    rename=columns,
                )

            if self.index_type in ["timestamp", "unix"] and all(pd.isna(d) for d in [start, end]):
                now = pd.Timestamp.now(tz=self.timezone)
                index = data.index.tz_convert(self.timezone).get_indexer([now], method="nearest")
                data = data.iloc[[index[-1]], :]

            results = []
            for resource in resources:
                if resource.id in data.columns:
                    results.append(data.loc[:, resource.id].copy())
                    continue

                resource_column = resource.get("column", default=resource.key)
                if resource_column not in data.columns:
                    results.append(pd.Series(name=resource.id))
                    continue
                resource_data = data.loc[:, resource_column].copy()
                resource_data.name = resource.id
                results.append(resource_data)
            return pd.concat(results, axis="columns")

        except IOError as e:
            raise ConnectionException(self, str(e))

    # noinspection PyTypeChecker
    def read_first(self, resources: Resources) -> Optional[pd.DataFrame]:
        columns = {r.name: r.id for r in self.resources if "name" in r}
        columns.update({r.name: r.id for r in resources if "name" in r})
        columns.update({column: key for key, column in self.columns.items()})
        try:
            if self._data is not None:
                data = self._data
            else:
                files = csv.get_files(
                    self._data_dir,
                    self.freq,
                    self.format,
                    timezone=self.timezone,
                )
                if len(files) == 0:
                    return None
                data = csv.read_file(
                    files[0],
                    index_column=self.index_column,
                    index_type=self.index_type,
                    timezone=self.timezone,
                    separator=self.separator,
                    decimal=self.decimal,
                    rename=columns,
                )
            if data is None or data.empty:
                return None
            results = []
            for resource in resources:
                if resource.id in data.columns:
                    results.append(data.loc[:, resource.id].copy())
                    continue

                resource_column = resource.get("column", default=resource.key)
                if resource_column not in data.columns:
                    results.append(pd.Series(name=resource.id))
                    continue
                resource_data = data.loc[:, resource_column].copy()
                resource_data.name = resource.id
                results.append(resource_data)
            return pd.concat(results, axis="columns").head(1)

        except IOError as e:
            raise ConnectionException(self, str(e))

    # noinspection PyTypeChecker
    def read_last(self, resources: Resources) -> Optional[pd.DataFrame]:
        columns = {r.name: r.id for r in self.resources if "name" in r}
        columns.update({r.name: r.id for r in resources if "name" in r})
        columns.update({column: key for key, column in self.columns.items()})
        try:
            if self._data is not None:
                data = self._data
            else:
                files = csv.get_files(
                    self._data_dir,
                    self.freq,
                    self.format,
                    timezone=self.timezone,
                )
                if len(files) == 0:
                    return None
                data = csv.read_file(
                    files[-1],
                    index_column=self.index_column,
                    index_type=self.index_type,
                    timezone=self.timezone,
                    separator=self.separator,
                    decimal=self.decimal,
                    rename=columns,
                )
            if data is None or data.empty:
                return None
            results = []
            for resource in resources:
                if resource.id in data.columns:
                    results.append(data.loc[:, resource.id].copy())
                    continue

                resource_column = resource.get("column", default=resource.key)
                if resource_column not in data.columns:
                    results.append(pd.Series(name=resource.id))
                    continue
                resource_data = data.loc[:, resource_column].copy()
                resource_data.name = resource.id
                results.append(resource_data)
            return pd.concat(results, axis="columns").tail(1)

        except IOError as e:
            raise ConnectionException(self, str(e))

    def write(self, data: pd.DataFrame) -> None:
        columns = {r.id: r.name for r in self.resources if "name" in r}
        columns.update(self.columns)
        kwargs = {
            "timezone": self.timezone,
            "separator": self.separator,
            "decimal": self.decimal,
            "override": self.override,
            "rename": columns,
        }
        data.index.name = self.index_column

        if self.slice:
            csv.write_files(data, self._data_dir, self.freq, self.format, **kwargs)
        else:
            csv_file = os.path.join(self._data_dir, data.index[0].strftime(self.format) + ".csv")
            csv.write_file(data, csv_file, **kwargs)
