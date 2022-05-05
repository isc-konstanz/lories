# -*- coding: utf-8 -*-
"""
    th-e-core.io.csv
    ~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

import os
import copy
import pytz as tz
import datetime as dt
import pandas as pd

# noinspection PyProtectedMember
import th_e_core.io._var as var
from th_e_core.io import Database
from th_e_core.tools import to_bool, to_int, convert_timezone
from dateutil.relativedelta import relativedelta


class CsvDatabase(Database):

    # noinspection PyShadowingBuiltins
    def __init__(self, dir=os.getcwd(), file=None, format='%Y%m%d_%H%M%S',
                 index_column='Time', index_unix=False, merge=False,
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

        self.columns = var.COLUMNS

    def exists(self,
               start: pd.tslib.Timestamp | dt.datetime = None,
               file: str = None,
               subdir: str = '', **_):

        if file is None:
            file = self.file
        if file is None and start is not None:
            start = convert_timezone(start, self.timezone)
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
            end = pd.Timestamp(convert_timezone(end, self.timezone))
            start = pd.Timestamp(convert_timezone(start, self.timezone))
            date = start.round('{hours}h'.format(hours=self.interval))
            if date > start:
                date -= relativedelta(hours=self.interval)

            path = os.path.join(self.dir, subdir)
            data = self._read_file(os.path.join(path, date.strftime(self.format) + '.csv'), **kwargs)
            if end is not None:
                def next_date() -> dt.datetime:
                    return (date + relativedelta(hours=self.interval)).round('{hours}h'.format(hours=self.interval))

                date = next_date()
                while date <= end:
                    if self.exists(date, subdir=subdir):
                        file = date.strftime(self.format) + '.csv'
                        file_path = os.path.join(path, file)
                        file_data = self._read_file(file_path, **kwargs)

                        columns_energy = [column for column in var.ENERGY.keys() if column in file_data.columns]
                        for column in columns_energy:
                            # TODO: verify if the energy values are continuously integrated or timestep deltas
                            file_data.loc[:, column] += data.loc[data.index[-1], column]

                        data = data.combine_first(file_data)
                    date = next_date()

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
                data[index_column] = pd.to_datetime(data[index_column], infer_datetime_format=True)

            data.set_index(index_column, verify_integrity=True, inplace=True)
            data.index.name = 'time'

            if not hasattr(data.index, 'tzinfo'):
                data[index_column] = data.index
                data[index_column] = data[index_column].apply(lambda t: t.astimezone(tz.utc).replace(tzinfo=None))
                data.set_index(index_column, verify_integrity=True, inplace=True)
                data.index.name = 'time'
                data.index = data.index.tz_localize(tz.utc).tz_convert(self.timezone)

            if data.index.tzinfo is None or data.index.tzinfo.utcoffset(data.index) is None:
                data.index = data.index.tz_localize(self.timezone, ambiguous="infer")
            elif data.index.tzinfo != self.timezone:
                data.index = data.index.tz_convert(self.timezone)

            # noinspection PyProtectedMember
            data = data.rename(columns=var._DEPRECATION)
            data = data.rename(columns=dict(
                [(value, key) for key, value in self.columns.items()]
            ))

        return data

    def write(self,
              data: pd.DataFrame,
              start: pd.Timestamp | dt.datetime = None,
              end:   pd.Timestamp | dt.datetime = None,
              file: str = None,
              subdir: str = '',
              split_days: bool = False,
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
                start = convert_timezone(start, self.timezone)

            if end is None:
                end = data.index[-1]
            else:
                end = convert_timezone(end, self.timezone)

            if file is None:
                file = self.file
            if file is None:
                file = start.strftime(self.format) + '.csv'
            file_path = os.path.join(path, file)

            if split_days:
                delta_day = dt.timedelta(hours=23, minutes=59, seconds=59, microseconds=999999)
                time_day = start.replace(hour=0, minute=0, second=0, microsecond=0)
                if time_day < start and time_day.day != start.day:
                    time_day += relativedelta(days=1)
                while time_day < end:
                    file = time_day.strftime(self.format) + '.csv'
                    file_path = os.path.join(path, file)
                    file_data = data[time_day:time_day + delta_day]

                    columns_energy = [column for column in var.ENERGY.keys() if column in file_data.columns]
                    if len(columns_energy) > 0:
                        for column in columns_energy:
                            # TODO: verify if the energy values are continuously integrated or timestep deltas
                            file_data.loc[:, column] -= file_data.loc[file_data.index[0], column]

                    self._write_file(file_path, file_data, **kwargs)

                    time_next = time_day + relativedelta(days=1)
                    time_next = time_next.replace(hour=0, minute=0, second=0, microsecond=0)
                    if time_next <= time_day:
                        time_next = time_day + relativedelta(days=2)
                        time_next = time_next.replace(hour=0, minute=0, second=0, microsecond=0)
                    time_day = time_next
            else:
                self._write_file(file_path, data.loc[start:end], **kwargs)

    def _write_file(self,
                    path: str,
                    data: pd.DataFrame,
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

        data = data[[column for column in self.columns.keys() if column in data.columns]]
        data = data.rename(columns=self.columns)
        data.index.name = 'Time'
        data.to_csv(path, sep=self.separator, decimal=self.decimal, encoding=encoding)


# noinspection PyProtectedMember
def write_csv(system, data, file):
    system_dir = system.configs['General']['data_dir']
    database = copy.deepcopy(system._database)
    database.dir = system_dir
    # database.format = '%Y%m%d'
    database.enabled = True
    data_file = os.path.join(database.dir, file + '.csv')
    data_dir = os.path.dirname(data_file)

    if not os.path.isdir(data_dir):
        os.makedirs(data_dir, exist_ok=True)

    data.to_csv(data_file, sep=database.separator, decimal=database.decimal, encoding='utf-8-sig')
