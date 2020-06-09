# -*- coding: utf-8 -*-
"""
    th-e-core.database
    ~~~~~~~~~~~~~~~~~~
    
    
"""
import logging
logger = logging.getLogger(__name__)

import os
import pytz as tz
import datetime as dt
import pandas as pd

from abc import ABC, abstractmethod


class Database(ABC):

    def __init__(self, enabled='true', timezone='UTC', **_):
        self.enabled = enabled.lower() == 'true'
        self.timezone = tz.timezone(timezone)

    @staticmethod
    def open(configs, **kwargs):
        dbargs = dict(configs.items('Database'))
        
        database_type = dbargs['type'].lower()
        if database_type == 'sql':
            database_tables = dict(configs.items('Tables'))
            return SqlDatabase(**dbargs, **kwargs, tables=database_tables)
        elif database_type == 'csv':
            return CsvDatabase(**dbargs, **kwargs)
        else:
            raise ValueError('Invalid database type argument')

    @abstractmethod
    def get(self, start, end=None, **kwargs):
        """ 
        Retrieve data for a specified time interval of a set of data feeds
        
        :param key: 
            the key for which the values will be looked up for.
        :type key: 
            :class: list of string
        
        :param start: 
            the time from which on values will be looked up for.
            For many applications, passing datetime.datetime.now() will suffice.
        :type start: 
            :class:`pandas.tslib.Timestamp` or datetime
        
        :param end: 
            the time until which values will be looked up for.
        :type end: 
            :class:`pandas.tslib.Timestamp` or datetime
        
        :returns: 
            the retrieved values, indexed in a specific time interval.
        :rtype: 
            :class:`pandas.DataFrame`
        """
        pass

    @abstractmethod
    def persist(self, data, **kwargs):
        """ 
        Persists a set of data values, to persistently store them on the server
        
        :param data: 
            the data set to be persisted
        :type data: 
            :class:`pandas.DataFrame`
        """
        pass

    def close(self, **kwargs):
        """ 
        Closes the database and cleans up all resources
        
        """
        pass


class SqlDatabase(Database):

    def __init__(self, host="127.0.0.1", port=3306, 
                 user='root', password='', database='emonpv',
                 tables=dict(), interval=24, 
                 **kwargs):
        
        super().__init__(**kwargs)
        
        self.interval = _int(interval)
        self.tables = tables
        
        import mysql.connector
        self.connector = mysql.connector.connect(
            host=host,
            port=_int(port),
            user=user,
            passwd=password,
            database=database
        )

    def exists(self, **kwargs):
        data = self.get(**kwargs)
        return not data.empty

    def get(self, start=None, end=None, resolution=None, **kwargs):
        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
        data = pd.DataFrame()
        
        for column, table in self.tables.items():
            cursor = self.connector.cursor()
            select = "SELECT time, data FROM {0} WHERE ".format(table)
            if end is None:
                select += "time >= %s ORDER BY time ASC"
                cursor.execute(select, ((start.astimezone(tz.UTC)-epoch).total_seconds(),))
            else:
                select += "time BETWEEN %s AND %s ORDER BY time ASC"
                cursor.execute(select, 
                               ((start.astimezone(tz.UTC)-epoch).total_seconds(), 
                                (end.astimezone(tz.UTC)-epoch).total_seconds()))
            
            times = []
            values = []
            for timestamp, value in cursor.fetchall():
                time = tz.utc.localize(dt.datetime.fromtimestamp(timestamp))
                times.append(time)
                values.append(value)
            
            result = pd.DataFrame(data=values, index=times, columns=[column])
            data = pd.concat([data, result], axis=1)
        
        if resolution is not None and resolution > 900:
            offset = (start - start.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds() % resolution
            data = data.resample(str(int(resolution))+'s', base=offset).sum()
        
        return data

    def persist(self, data, **_):
        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
        
        cursor = self.connector.cursor()
        for column in data.columns:
            insert = "INSERT INTO {0} (time,data) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE data=VALUES(data)".format(self.tables[column])
            values = []
            for index, value in data[column].items():
                time = (index.astimezone(tz.UTC)-epoch).total_seconds()
                values.append((time, value))
            
            cursor.executemany(insert, values)
        
        self.connector.commit()


class CsvDatabase(Database):

    def __init__(self, dir=os.getcwd(), file=None, format='%Y%m%d_%H%M%S', #@ReservedAssignment
                 index_column='time', index_unix=False, merge=False, 
                 interval=24, timezone='UTC', decimal='.', separator=',', 
                 **kwargs):
        
        super().__init__(**kwargs)
        
        self.dir = dir
        self.file = file
        
        self.format = format
        self.index_column = index_column
        self.index_unix = _bool(index_unix)
        
        self.interval = _int(interval)
        self.merge = _bool(merge)
        
        self.timezone = tz.timezone(timezone)
        self.decimal = decimal
        self.separator = separator

    def exists(self, date, subdir='', **_):
        return os.path.exists(os.path.join(self.dir, subdir, 
                                           self.file if self.file is not None else date.strftime(self.format) + '.csv'))

    def get(self, start=None, end=None, resolution=None, subdir='', **kwargs):
        if self.file is not None:
            data = self._read_file(os.path.join(self.dir, subdir, self.file), **kwargs)
        
        else:
            data = self._read_file(os.path.join(self.dir, subdir, start.strftime(self.format) + '.csv'), **kwargs)
            if end is not None:
                time = start
                while time <= end:
                    if self.exists(time, subdir):
                        data = data.combine_first(self._read_file(os.path.join(self.dir, subdir, time.strftime(self.format) + '.csv'), **kwargs))
                    
                    time += dt.timedelta(hours=self.interval)
        
        if resolution is not None and resolution > 900:
            offset = (start - start.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds() % resolution
            data = data.resample(str(int(resolution))+'s', base=offset).sum()
            
            if end is not None:
                end += dt.timedelta(seconds=resolution)
        
        if end is not None:
            if start > end:
                return data.truncate(before=start).head(1)
            
            return data.loc[start:end]
        
        return data

    def persist(self, data, start=None, end=None, file=None, subdir='', **kwargs): #@UnusedVariable
        if data is not None and self.enabled:
            if start is None:
                start = data.index[0]
            if end is None:
                end = data.index[-1]
            if file is None:
                file = start.strftime(self.format) + '.csv'
            
            path = os.path.join(self.dir, subdir)
            if not os.path.exists(path):
                os.makedirs(path)
            
            self._write_file(os.path.join(path, file), data.loc[start:end,:])

    def _write_file(self, path, data, encoding='utf-8'):
        if self.merge and os.path.isfile(path):
            index = data.index.name
            csv = pd.read_csv(path, sep=self.separator, decimal=self.decimal, encoding=encoding, index_col=index, parse_dates=[index])
            if not csv.empty:
                csv.index = csv.index.tz_localize(tz.utc)
                
                if all(name in list(csv.columns) for name in list(data.columns)):
                    data = data.combine_first(csv)
                else:
                    data = pd.concat([csv, data], axis=1)
        
        data.to_csv(path, sep=self.separator, decimal=self.decimal, encoding=encoding)

    def _read_file(self, path, **kwargs):
        """
        Reads the content of a specified CSV file.
        
        :param path: 
            the full path to the CSV file.
        :type path:
            string or unicode
        
        :param index_column: 
            the name of the column, that will be used as index. The index will be assumed 
            to be a time format, that will be parsed and localized.
        :type index_column:
            string or unicode
        
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
        
        csv.index.name = 'time'
        
        return csv #.tz_convert(self.timezone)


def _bool(v):
    if isinstance(v, str):
        return v.lower() == 'true'
    
    return v

def _float(v):
    if isinstance(v, str):
        return float(v)
    
    return v

def _int(v):
    if isinstance(v, str):
        return int(v)
    
    return v

