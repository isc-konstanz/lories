# -*- coding: utf-8 -*-
"""
loris.connectors.csv
~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import os
from typing import Mapping, Optional, Tuple

import pandas as pd
import pytz as tz
from loris.connectors import Connector
from loris.core import ConfigurationException, Configurations, Resources
from loris.io import csv
from loris.util import _parse_freq, ceil_date, floor_date, resample, to_timezone


# noinspection PyShadowingBuiltins
class CsvConnector(Connector):
    TYPE: str = "csv"

    _data: Optional[pd.DataFrame] = None
    _data_path: Optional[str] = None
    _data_dir: str

    index_column: str = "timestamp"
    index_unix: bool = False

    resolution: int = None

    override: bool = False
    slice: bool = False

    freq: str = "D"
    format: str = None
    suffix: Optional[str] = None

    timezone: Optional[tz.BaseTzInfo] = None

    decimal: str = "."
    separator: str = ","

    columns: Mapping[str, str] = {}

    @property
    def type(self) -> str:
        return self.TYPE

    # noinspection PyTypeChecker
    def configure(self, configs: Configurations) -> None:
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

        self.index_column = configs.get("index_column", default=CsvConnector.index_column)
        self.index_unix = configs.get_bool("index_unix", default=CsvConnector.index_unix)

        # TODO: Validate if minutely default resolution is sufficient
        resolution = configs.get_int("resolution", default=CsvConnector.resolution)
        if resolution is not None:
            resolution *= 60
        self.resolution = resolution

        self.override = configs.get_bool("override", default=CsvConnector.override)
        self.slice = configs.get_bool("slice", default=CsvConnector.slice)

        self.freq = _parse_freq(configs.get("freq", default=CsvConnector.freq))

        format = configs.get("format", default=CsvConnector.format)
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

        self.suffix = configs.get("suffix", default=CsvConnector.suffix)
        if self.suffix is not None:
            self.format += f"_{self.suffix}"

        timezone = configs.get("timezone", CsvConnector.timezone)
        if isinstance(timezone, str):
            timezone = tz.timezone(timezone)
        self.timezone = to_timezone(timezone)

        self.decimal = configs.get("decimal", CsvConnector.decimal)
        self.separator = configs.get("separator", CsvConnector.separator)

        # TODO: Implement flag if pretty printing should be used or not
        self.columns = configs.get("columns", default=CsvConnector.columns)

    def connect(self, resources: Resources) -> None:
        if not os.path.isdir(self._data_dir):
            os.makedirs(self._data_dir, exist_ok=True)
        if self._data_path is not None:
            self._data = csv.read_file(
                self._data_path,
                index_column=self.index_column,
                index_unix=self.index_unix,
                timezone=self.timezone,
                separator=self.separator,
                decimal=self.decimal,
                rename=self.columns,
            )

    def disconnect(self) -> None:
        self._data = None

    def is_connected(self) -> bool:
        return True

    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> pd.DataFrame:
        columns = {c.name: c.id for c in self.channels if "name" in c}
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
                    index_unix=self.index_unix,
                    timezone=self.timezone,
                    separator=self.separator,
                    decimal=self.decimal,
                    rename=columns,
                )

            if self.resolution is not None:
                data = resample(data, self.resolution)

            if all(pd.isna(d) for d in [start, end]):
                now = pd.Timestamp.now(tz=self.timezone)
                index = data.index.tz_convert(self.timezone).get_indexer([now], method="nearest")
                data = data.iloc[[index[-1]], :]

            data = data.rename(columns={r.id if "column" not in r else r.column: r.uuid for r in resources})
            return data.loc[:, [r.uuid for r in resources if r.uuid in data.columns]]

        except IOError:
            # TODO: Return more informative ChannelStates or raise ConnectionException (?)
            return pd.DataFrame()

    def write(self, data: pd.DataFrame) -> None:
        columns = {r.uuid: r.name for r in self.resources if "name" in r}
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
