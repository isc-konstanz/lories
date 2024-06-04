# -*- coding: utf-8 -*-
"""
    loris.connectors.csv
    ~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations

import datetime as dt
import os
from typing import Optional

import pandas as pd
import pytz as tz
from loris.channels import Channels
from loris.configs import ConfigurationException, Configurations
from loris.connectors import Connector
from loris.io import csv
from loris.util import _parse_freq, resample


# noinspection PyShadowingBuiltins
class CsvConnector(Connector):
    TYPE: str = "csv"

    _data: Optional[pd.DataFrame] = None

    def __configure__(self, configs: Configurations) -> None:
        data_dir = configs.get("dir", default=os.getcwd())
        if "~" in data_dir:
            data_dir = os.path.expanduser(data_dir)
        if not os.path.isabs(data_dir):
            data_dir = os.path.join(configs.dirs.data, data_dir)
        self._data_dir = data_dir

        data_path = configs.get("file", default=None)
        if data_path is not None and not os.path.isabs(data_path):
            data_path = os.path.join(data_dir, data_path)
        self._data_path = data_path

        self.index_column = configs.get("index_column", default="timestamp")
        self.index_unix = configs.get_bool("index_unix", default=False)

        # TODO: Validate if minutely default resolution is sufficient
        resolution = configs.get_int("resolution", default=None)
        if resolution is not None:
            resolution *= 60
        self.resolution = resolution

        self.override = configs.get_bool("override", default=False)
        self.slice = configs.get_bool("slice", default=True)

        self.freq = _parse_freq(configs.get("freq", default="D"))

        format = configs.get("format", default=None)
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

        self.suffix = configs.get("suffix", default=None)
        if self.suffix is not None:
            self.format += f"_{self.suffix}"

        timezone = configs.get("timezone", None)
        if isinstance(timezone, str):
            timezone = tz.timezone(timezone)
        self.timezone = timezone

        self.decimal = configs.get("decimal", ".")
        self.separator = configs.get("separator", ",")

        # TODO: Implement flag if pretty printing should be used or not
        self.columns = configs.get("columns", default={})

    def __connect__(self, channels: Channels) -> None:
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

    def __disconnect__(self) -> None:
        self._data = None

    def read(
        self,
        channels: Channels,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> None:
        columns = {c.name: c.id for c in channels if "name" in c}
        columns.update({column: key for key, column in self.columns.items()})

        if self._data is not None:
            data = self._data
        else:
            data = csv.read_files(
                self._data_dir,
                self.freq,
                self.format,
                start,
                end,
                index_column=self.index_column,
                index_unix=self.index_unix,
                timezone=self.timezone,
                separator=self.separator,
                decimal=self.decimal,
                rename=columns,
            )

        if self.resolution is not None:
            data = resample(data, self.resolution)
        return data.loc[start:end, :]

    def write(self, channels: Channels) -> None:
        columns = {c.id: c.name for c in channels if "name" in c}
        columns.update(self.columns)
        kwargs = {
            "timezone": self.timezone,
            "separator": self.separator,
            "decimal": self.decimal,
            "override": self.override,
            "rename": columns,
        }
        if self.slice:
            data = channels.to_frame()
            data.index.name = self.index_column
            csv.write_files(channels.to_frame(), self._data_dir, self.freq, self.format, **kwargs)
        else:
            for data_time, data_channels in channels.groupby("timestamp"):
                data = data_channels.to_frame()
                data.index.name = self.index_column

                csv_file = os.path.join(self._data_dir, data_time.strftime(self.format) + ".csv")
                csv.write_file(data, csv_file, **kwargs)

    def is_connected(self) -> bool:
        return True
