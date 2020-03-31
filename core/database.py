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

    def __init__(self, disabled='false', timezone='UTC', **_):
        self.disabled = disabled.lower() == 'true'
        self.timezone = tz.timezone(timezone)

    @staticmethod
    def open(configs, **kwargs):
        dbargs = dict(configs.items('Database'))
        
        database_type = dbargs['type'].lower()
        if database_type == 'csv':
            return CsvDatabase(**dbargs, **kwargs)
        else:
            raise ValueError('Invalid database type argument')

    @abstractmethod
    def get(self, start, stop=None, **kwargs):
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
        
        :param stop: 
            the time until which values will be looked up for.
        :type stop: 
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


class CsvDatabase(Database):

    def __init__(self, dir = os.getcwd(), format = '%Y%m%d_%H%M%S',  #@ReservedAssignment
                 index_column = 'time', index_unix = False, merge = False, 
                 interval = 24, decimal = '.', separator = ',', **kwargs):
        
        super().__init__(**kwargs)
        
        self.dir = dir
        
        self.format = format
        self.index_column = index_column
        self.index_unix = _bool(index_unix)
        
        self.interval = _int(interval)
        self.merge = _bool(merge)
        
        self.decimal = decimal
        self.separator = separator

    def exists(self, time, subdir='', *_):
        return os.path.exists(os.path.join(self.dir, subdir, time.strftime(self.format) + '.csv'))

    def get(self, start=None, stop=None, interval=None, subdir='', **kwargs):
        data = pd.DataFrame()
        if stop is None:
            stop = start
        stop += dt.timedelta(hours=self.interval) - dt.timedelta(seconds=1)
        
        time = start
        while time <= stop:
            if self.exists(time, subdir):
                data = data.combine_first(self._read_file(os.path.join(self.dir, subdir, time.strftime(self.format) + '.csv'), **kwargs))
            
            time += dt.timedelta(hours=self.interval)
        
        if interval is not None and interval > 900:
            offset = (start - start.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds() % interval
            data = data.resample(str(int(interval))+'s', base=offset).sum()
            stop += dt.timedelta(seconds=interval)
        
        if stop is not None:
            if start > stop:
                return data.truncate(before=start).head(1)
            
            return data.loc[start:stop]
        
        return data

    def persist(self, data, time=None, file=None, subdir='', **kwargs):
        if data is not None and not self.disabled:
            if time is None:
                time = data.index[0]
            if file is None:
                file = time.strftime(self.format) + '.csv'
            
            path = os.path.join(self.dir, subdir)
            if not os.path.exists(path):
                os.makedirs(path)
            
            self._write_file(os.path.join(path, file), data, **kwargs)

    def _write_file(self, path, data, encoding='utf-8', **kwargs):
        if self.merge and os.path.isfile(path):
            index = data.index.name
            csv = pd.read_csv(path, sep=self.separator, decimal=self.decimal, encoding=encoding, index_col=index, parse_dates=[index], **kwargs)
            if not csv.empty:
                csv.index = csv.index.tz_localize(tz.utc)
                
                if all(name in list(csv.columns) for name in list(data.columns)):
                    data = data.combine_first(csv)
                else:
                    data = pd.concat([csv, data], axis=1)
        
        data.to_csv(path, sep=self.separator, decimal=self.decimal, encoding=encoding, **kwargs)

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
        
        return csv


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

