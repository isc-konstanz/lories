# -*- coding: utf-8 -*-
"""
    th-e-core.database
    ~~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

import os
import pytz as tz
import datetime as dt
import pandas as pd

from th_e_core.iotools import Database
from th_e_core.tools import to_bool, to_int


class CsvDatabase(Database):

    # noinspection PyShadowingBuiltins
    def __init__(self, dir=os.getcwd(), file=None, format='%Y%m%d_%H%M%S',
                 index_column='time', index_unix=False, merge=False,
                 interval=24, timezone='UTC', decimal='.', separator=',',
                 **kwargs):

        super().__init__(**kwargs)

        if "~" in dir:
            dir = os.path.expanduser(dir)
        self.dir = dir

        if file is not None and not os.path.isabs(file):
            file = os.path.join(dir, file)
        self.file = file

        self.format = format
        self.index_column = index_column
        self.index_unix = to_bool(index_unix)

        self.interval = to_int(interval)
        self.merge = to_bool(merge)

        self.timezone = tz.timezone(timezone)
        self.decimal = decimal
        self.separator = separator

    def exists(self,
               start: pd.tslib.Timestamp | dt.datetime = None,
               file: str = None,
               subdir: str = '', **_):

        if file is None:
            file = self.file
        if file is None and start is not None:
            file = start.strftime(self.format) + '.csv'
        if file is None:
            return False

        return os.path.isfile(os.path.join(self.dir, subdir, file))

    def read(self,
             start: pd.Timestamp | dt.datetime = None,
             end:   pd.Timestamp | dt.datetime = None,
             resolution: int = None,
             file: str = None,
             subdir: str = '',
             **kwargs) -> pd.DataFrame:

        if file is None:
            file = self.file
        if file is not None:
            data = self._read_file(os.path.join(self.dir, subdir, file), **kwargs)

        else:
            data = self._read_file(os.path.join(self.dir, subdir, start.strftime(self.format) + '.csv'), **kwargs)
            if end is not None:
                date = start
                while date <= end:
                    if self.exists(date, subdir=subdir):
                        date_str = date.strftime(self.format)
                        data_file = date_str + '.csv'
                        data = data.combine_first(self._read_file(os.path.join(self.dir, subdir, data_file), **kwargs))

                    date += dt.timedelta(hours=self.interval)

        if resolution is not None and resolution > 900:
            offset = (start - start.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds() % resolution
            data = data.resample(str(int(resolution))+'s', base=offset).sum()

            if end is not None:
                end += dt.timedelta(seconds=resolution)

        if end is not None:
            if start > end:
                return data.truncate(before=start).head(1)

            return data[(data.index >= start) & (data.index <= end)]

        return data

    def write(self,
              data: pd.DataFrame,
              start: pd.Timestamp | dt.datetime = None,
              end:   pd.Timestamp | dt.datetime = None,
              file: str = None,
              subdir: str = '',
              **kwargs) -> None:

        if data is not None and self.enabled:
            if start is None:
                start = data.index[0]
            if end is None:
                end = data.index[-1]
            if file is None:
                file = self.file
            if file is None:
                file = start.strftime(self.format) + '.csv'

            path = os.path.join(self.dir, subdir)
            if not os.path.exists(path):
                os.makedirs(path)

            self._write_file(os.path.join(path, file), data.loc[start:end], **kwargs)

    def _write_file(self,
                    path: str,
                    data: pd.DataFrame,
                    encoding: str = 'utf-8-sig',
                    **kwargs):

        if self.merge and os.path.isfile(path):
            index = data.index.name
            csv = pd.read_csv(path,
                              sep=self.separator,
                              decimal=self.decimal,
                              encoding=encoding,
                              index_col=index,
                              parse_dates=[index])

            if not csv.empty:
                csv.index = csv.index.tz_localize(tz.utc)

                if all(name in list(csv.columns) for name in list(data.columns)):
                    data = data.combine_first(csv)
                else:
                    data = pd.concat([csv, data], axis=1)

        data.to_csv(path, sep=self.separator, decimal=self.decimal, encoding=encoding, **kwargs)

    def _read_file(self, path: str, **kwargs):
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
        csv = pd.read_csv(path, sep=self.separator, decimal=self.decimal,
                          index_col=self.index_column, parse_dates=[self.index_column], **kwargs)

        if not csv.empty:
            if self.index_unix:
                csv.index = pd.to_datetime(csv.index, unit='ms')

            if csv.index.tzinfo is None or csv.index.tzinfo.utcoffset(csv.index) is None:
                csv.index = csv.index.tz_localize(tz.utc)

        # csv.index.name = 'time'

        return csv  # .tz_convert(self.timezone)
