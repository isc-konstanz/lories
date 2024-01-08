# -*- coding: utf-8 -*-
"""
    corsys.io.csv
    ~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import List

import os
import glob
import pytz as tz
import datetime as dt
import pandas as pd

# noinspection PyProtectedMember
from . import _var as var
from . import Database, DatabaseException
from ..tools import to_bool, to_int, to_timedelta, to_date, floor_date, ceil_date, resample


class CsvDatabase(Database):

    # noinspection PyShadowingBuiltins
    def __init__(self,
                 dir=os.getcwd(),
                 file=None,
                 index_column='Time',
                 index_unix=False,
                 resolution=None,
                 merge=False,
                 freq='D',
                 format=None,
                 timezone=tz.UTC,
                 decimal='.',
                 separator=',',
                 columns=None,
                 **kwargs):
        super().__init__(**kwargs)

        if "~" in dir:
            dir = os.path.expanduser(dir)
        self.dir = dir

        if file is not None and not os.path.isabs(file):
            file = os.path.join(dir, file)
        self.file = file

        self.index_column = index_column
        self.index_unix = to_bool(index_unix)

        self.resolution = to_int(resolution)*60 if resolution is not None else None
        self.merge = to_bool(merge)
        self.freq = freq
        if format is not None:
            self.format = format
        elif self.freq == 'Y':
            self.format = '%Y'
        elif self.freq == 'M':
            self.format = '%Y-%m'
        elif any([self.freq.endswith(s) for s in ['D', 'H', 'T']]):
            self.format = '%Y%m%d_%H%M%S'
        else:
            raise ValueError(f"Invalid frequency: {freq}")

        if isinstance(timezone, str):
            timezone = tz.timezone(timezone)
        self.timezone = timezone
        self.decimal = decimal
        self.separator = separator

        if columns is None:
            columns = {}
        self.columns = var.COLUMNS
        self.columns.update(columns)

    def exists(self,
               start: pd.Timestamp | dt.datetime | str = None,
               end: pd.Timestamp | dt.datetime | str = None,
               file: str = None,
               subdir: str = '', **_):

        files = self._get_files(start, end, file, subdir)
        return all([os.path.isfile(f) for f in files])

    def read(self,
             start: pd.Timestamp | dt.datetime | str = None,
             end:   pd.Timestamp | dt.datetime | str = None,
             resolution: int = None,
             file: str = None,
             subdir: str = '',
             **kwargs) -> pd.DataFrame:

        data = pd.DataFrame()
        start = to_date(start, self.timezone)
        end = to_date(end, self.timezone)
        end = ceil_date(end, self.timezone)  # TODO: Check if ceiling this is unproblematic
        for file in self._get_files(start, end, file, subdir):
            if not data.empty and (end is not None and data.index[-1] > end):
                break
            if not os.path.isfile(file):
                raise DatabaseException('Unable to find file: ' + file)

            file_data = self._read_file(file, **kwargs)

            if not data.empty:
                columns_energy = [column for column in self.columns.keys()
                                  if '_energy' in column and column in file_data.columns]
                for column in columns_energy:
                    # TODO: verify if the energy values are continuously integrated or timestep deltas
                    file_data.loc[:, column] = file_data[column] + data.loc[data.index[-1], column]

            data = file_data.combine_first(data)

        if resolution is None:
            resolution = self.resolution
        if resolution is not None:
            data = resample(data, resolution)

        if end is not None:
            if start > end:
                return data.truncate(before=start).head(1)

            return data[(data.index >= start) & (data.index <= end)]

        return data

    def _read_file(self, path: str, **_):
        """
        Reads the content of a specified CSV file.

        :param path:
            the full path to the CSV file.
        :type path:
            string


        :param unix:
            the flag, if the index column contains UNIX timestamps that need to be parsed accordingly.
        :type unix:
            boolean


        :returns:
            the retrieved columns, indexed by their date
        :rtype:
            :class:`pandas.DataFrame`
        """
        data = pd.read_csv(path, sep=self.separator, decimal=self.decimal)
        if not data.empty:
            index_column = self.index_column
            if index_column not in data.columns:
                index_column = index_column.lower()

            if self.index_unix:
                data[index_column] = pd.to_datetime(data[index_column], unit='ms')
            else:
                data[index_column] = pd.to_datetime(data[index_column])

            data.set_index(index_column, verify_integrity=True, inplace=True)
            data.index.name = 'time'

            if not hasattr(data.index, 'tzinfo'):
                data[index_column] = data.index
                data[index_column] = data[index_column].apply(lambda t: t.astimezone(tz.utc).replace(tzinfo=None))
                data.set_index(index_column, verify_integrity=True, inplace=True)
                data.index.name = 'time'
                data.index = data.index.tz_localize(tz.utc).tz_convert(self.timezone)

            if hasattr(data.index, 'tzinfo') and data.index.tzinfo is not None:
                if data.index.tzinfo != self.timezone:
                    data.index = data.index.tz_convert(self.timezone)
            else:
                data.index = data.index.tz_localize(self.timezone, ambiguous="infer")

            # noinspection PyProtectedMember
            data = data.rename(columns=var._DEPRECATION)
            data = data.rename(columns=dict(
                [(value, key) for key, value in self.columns.items()]
            ))

        return data

    def write(self,
              data: pd.DataFrame,
              start: pd.Timestamp | dt.datetime | str = None,
              end:   pd.Timestamp | dt.datetime | str = None,
              file: str = None,
              subdir: str = '',
              split_data: bool = False,
              **kwargs) -> None:
        if data is not None and not data.empty and self.enabled:
            path = os.path.join(self.dir, subdir)
            if not os.path.exists(path):
                os.makedirs(path)

            if data.index.tzinfo is None or data.index.tzinfo.utcoffset(data.index) is None:
                data.index = data.index.tz_localize(tz.utc, ambiguous="infer")
            elif data.index.tzinfo != self.timezone:
                data.index = data.index.tz_convert(self.timezone)

            if start is None:
                start = data.index[0]
            else:
                start = to_date(start, self.timezone)
                data = data[data.index >= start]

            if end is None:
                end = data.index[-1]
            else:
                end = to_date(end, self.timezone)
                data = data[data.index <= end]

            if file is None:
                file = self.file
            if file is None:
                file = start.strftime(self.format) + '.csv'
            else:
                split_data = False

            if split_data:
                time_step = floor_date(start, freq=self.freq)

                def next_step() -> pd.Timestamp:
                    return floor_date(time_step + to_timedelta(self.freq), timezone=self.timezone, freq=self.freq)

                while time_step < end:
                    time_next = next_step()

                    file = time_step.strftime(self.format) + '.csv'
                    file_path = os.path.join(path, file)
                    file_data = data[(data.index >= time_step) & (data.index < time_next)].copy()

                    columns_energy = [column for column in self.columns.keys()
                                      if '_energy' in column and column in file_data.columns]
                    for column in columns_energy:
                        # TODO: verify if the energy values are continuously integrated or timestep deltas
                        file_data.loc[:, column] = file_data[column] - file_data.loc[file_data.index[0], column]

                    self._write_file(file_path, file_data, **kwargs)

                    time_step = time_next
            else:
                self._write_file(os.path.join(path, file), data, **kwargs)

    def _write_file(self,
                    path: str,
                    data: pd.DataFrame,
                    rename: bool = True,
                    encoding: str = 'utf-8-sig',
                    **_):
        if data.index.tzinfo is None or data.index.tzinfo.utcoffset(data.index) is None:
            data.index = data.index.tz_localize(self.timezone, ambiguous="infer")
        elif data.index.tzinfo != self.timezone:
            data.index = data.index.tz_convert(self.timezone)

        if self.merge and os.path.isfile(path):
            index = data.index.name
            csv = pd.read_csv(path,
                              sep=self.separator,
                              decimal=self.decimal,
                              encoding=encoding,
                              index_col=index,
                              parse_dates=[index])

            if not csv.empty:
                if csv.index.tzinfo is None or csv.index.tzinfo.utcoffset(data.index) is None:
                    csv.index = csv.index.tz_localize(tz.utc, ambiguous="infer")
                elif csv.index.tzinfo != self.timezone:
                    csv.index = csv.index.tz_convert(self.timezone)

                if all(name in list(csv.columns) for name in list(data.columns)):
                    data = data.combine_first(csv)
                else:
                    data = pd.concat([csv, data], axis=1)

        if rename:
            data = data[[column for column in self.columns.keys() if column in data.columns]]
            data = data.rename(columns=self.columns)
            data.index.name = 'Time'
        data.to_csv(path, sep=self.separator, decimal=self.decimal, encoding=encoding)

    def _get_files(self,
                   start: pd.Timestamp | dt.datetime | str,
                   end:   pd.Timestamp | dt.datetime | str,
                   file: str,
                   subdir: str) -> List[str]:

        path = os.path.join(self.dir, subdir)

        files = []
        if file is None:
            file = self.file
        if file is not None:
            if os.path.isabs(file):
                file_path = file
            else:
                file_path = os.path.join(self.dir, subdir, file)

            files.append(file_path)
        else:
            end = to_date(end, self.timezone)
            start = to_date(start, self.timezone)
            if start is None or end is None:
                filenames = [os.path.basename(f) for f in glob.glob(os.path.join(path, '*.csv'))]
                if len(filenames) > 0:
                    filenames.sort()
                    if start is None and end is None:
                        return [os.path.join(path, f) for f in filenames]

                    if start is None:
                        start_str = filenames[0].replace('.csv', '')
                        start = to_date(start_str, timezone=self.timezone, format=self.format)

                        if end is None:
                            end_str = filenames[-1].replace('.csv', '')
                            end = to_date(end_str, timezone=self.timezone, format=self.format)
                            end = ceil_date(end, timezone=self.timezone, freq=self.freq)

            date = floor_date(start, timezone=self.timezone, freq=self.freq)

            # noinspection PyShadowingNames
            def next_date() -> pd.Timestamp:
                next_date = floor_date(date + to_timedelta(self.freq), timezone=self.timezone, freq=self.freq)
                if next_date == date:
                    next_date += to_timedelta(self.freq)
                    next_offset = date.utcoffset() - next_date.utcoffset()
                    if next_offset.seconds > 0:
                        next_date = floor_date(next_date + next_offset, timezone=self.timezone, freq=self.freq)
                    else:
                        DatabaseException(f"Unable to increment date for freq '{self.freq}'")
                return next_date

            file = date.strftime(self.format) + '.csv'
            file_path = os.path.join(path, file)
            files.append(file_path)
            if end is not None:
                date = next_date()
                while date <= end:
                    file = date.strftime(self.format) + '.csv'
                    file_path = os.path.join(path, file)
                    files.append(file_path)
                    date = next_date()

        return files
