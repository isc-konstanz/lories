# -*- coding: utf-8 -*-
"""
lori.connectors.csv
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import os
from typing import Mapping, Optional, Tuple

import pandas as pd
from lori.connectors import ConnectionError, Database, register_connector_type
from lori.core.configs import ConfigurationError
from lori.io import csv
from lori.typing import Configurations, Resources, Timestamp
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
    pretty: bool = False

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
            raise ConfigurationError(f"Unknown index type: {self.index_type}")

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
            raise ConfigurationError(f"Invalid frequency: {self.freq}")

        self.suffix = configs.get("suffix", default=CsvDatabase.suffix)
        if self.suffix is not None:
            self.format += f"_{self.suffix}"

        self.decimal = configs.get("decimal", CsvDatabase.decimal)
        self.separator = configs.get("separator", CsvDatabase.separator)

        self.pretty = configs.get_bool("pretty", default=False)
        self.columns = configs.get("columns", default=CsvDatabase.columns)

    def _build_columns(self, resources: Optional[Resources] = None) -> Mapping[str, str]:
        columns = {r.id: self.columns[r.key] for r in resources if r.key in self.columns}
        if self.pretty:
            columns.update({r.id: r.full_name(unit=True) for r in self.resources if "name" in r})
            if resources is not None:
                columns.update({r.id: r.full_name(unit=True) for r in resources if "name" in r})
        else:
            columns.update({r.id: r.get("column", default=r.key) for r in self.resources})
            if resources is not None:
                columns.update({r.id: r.get("column", default=r.key) for r in resources})
        return columns

    def connect(self, resources: Resources) -> None:
        if not os.path.isdir(self._data_dir):
            os.makedirs(self._data_dir, exist_ok=True)
        try:
            if self._data_path is not None:
                self._data = csv.read_file(
                    self._data_path,
                    index_column=self.index_column,
                    index_type=self.index_type,
                    timezone=self.timezone,
                    separator=self.separator,
                    decimal=self.decimal,
                    rename=self._build_columns(resources),
                )
        except IOError as e:
            raise ConnectionError(self, str(e))

    def disconnect(self) -> None:
        self._data = None

    def is_connected(self) -> bool:
        return True

    def read(
        self,
        resources: Resources,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ) -> pd.DataFrame:
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
                )

            if self.index_type in ["timestamp", "unix"] and all(pd.isna(d) for d in [start, end]):
                now = pd.Timestamp.now(tz=self.timezone)
                index = data.index.tz_convert(self.timezone).get_indexer([now], method="nearest")
                data = data.iloc[[index[-1]], :]

            columns = self._build_columns(resources)
            results = []
            for resource in resources:
                resource_column = columns[resource.id]
                if resource_column not in data.columns:
                    results.append(pd.Series(name=resource.id))
                    continue
                resource_data = data.loc[:, resource_column].copy()
                resource_data.name = resource.id
                results.append(resource_data)
            return pd.concat(results, axis="columns")

        except IOError as e:
            raise ConnectionError(self, str(e))

    # noinspection PyTypeChecker
    def read_first(self, resources: Resources) -> Optional[pd.DataFrame]:
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
                    rename=self._build_columns(resources),
                )
            if data is None or data.empty:
                return None

            columns = self._build_columns(resources)
            results = []
            for resource in resources:
                resource_column = columns[resource.id]
                if resource_column not in data.columns:
                    results.append(pd.Series(name=resource.id))
                    continue
                resource_data = data.loc[:, resource_column].copy()
                resource_data.name = resource.id
                results.append(resource_data)
            return pd.concat(results, axis="columns").head(1)

        except IOError as e:
            raise ConnectionError(self, str(e))

    # noinspection PyTypeChecker
    def read_last(self, resources: Resources) -> Optional[pd.DataFrame]:
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
                    rename=self._build_columns(resources),
                )
            if data is None or data.empty:
                return None

            columns = self._build_columns(resources)
            results = []
            for resource in resources:
                resource_column = columns[resource.id]
                if resource_column not in data.columns:
                    results.append(pd.Series(name=resource.id))
                    continue
                resource_data = data.loc[:, resource_column].copy()
                resource_data.name = resource.id
                results.append(resource_data)
            return pd.concat(results, axis="columns").tail(1)

        except IOError as e:
            raise ConnectionError(self, str(e))

    def write(self, data: pd.DataFrame) -> None:
        columns = self._build_columns(self.resources)
        kwargs = {
            "timezone": self.timezone,
            "separator": self.separator,
            "decimal": self.decimal,
            "override": self.override,
            "rename": columns,
        }
        if self.pretty:
            data.index.name = self.index_column.title()
        else:
            data.index.name = self.index_column

        if self.slice:
            csv.write_files(data, self._data_dir, self.freq, self.format, **kwargs)
        else:
            csv_file = os.path.join(self._data_dir, data.index[0].strftime(self.format) + ".csv")
            csv.write_file(data, csv_file, **kwargs)
