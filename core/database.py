# -*- coding: utf-8 -*-
"""
    th-e-core.database
    ~~~~~~~~~~~~~~~~~~
    
    
"""
import logging
logger = logging.getLogger(__name__)

import os
import time
import pytz as tz
import datetime as dt
import pandas as pd
import numpy as np

from abc import ABC, abstractmethod


class Database(ABC):

    def __init__(self, configs, **_):
        self.disabled = configs.get('disabled', fallback='false').lower() == 'true'
        self.timezone = tz.timezone(configs.get('timezone', fallback='UTC'))

    @staticmethod
    def open(configs, **kwargs):
        datbase_type = configs['Database']['type'].lower()
        if datbase_type == 'emoncms':
            return EmoncmsDatabase(configs['Database'], **kwargs)
        
        elif datbase_type == 'csv':
            return CsvDatabase(configs['Database'], **kwargs)
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


class EmoncmsDatabase(Database):

    def __init__(self, configs, **kwargs):
        super().__init__(configs, **kwargs)
        from emonpy import Emoncms
        from configparser import ConfigParser
        
        emoncmsfile = configs['configs']
        emoncms = ConfigParser()
        emoncms.open(emoncmsfile)
        
        self.node = configs['node']
        self.connection = Emoncms(emoncms['address'], emoncms['authentication'])

    def get(self, system, start, stop, interval, **kwargs):
        pass

    def persist(self, system, data, *_):
        from emonpy import EmoncmsData
        
        if hasattr(system, 'apikey'):
            bulk = EmoncmsData(timezone=self.timezone)
            for key in data.columns:
                name = system.name.lower()
                
                if len(data.columns) > 1 and name != key.lower():
                    name += '_' + key.replace(' ', '_').lower()
                
                for time, value in data[key].items():
                    if value is not None and not np.isnan(value):
                        bulk.add(time, self.node, name, float(value))
            
            self.connection.persist(bulk, apikey=system.apikey)


class CsvDatabase(Database):

    def __init__(self, configs, **kwargs):
        super().__init__(configs, **kwargs)
        
        self.dir = configs.get('dir', fallback='.')
        self.merge = configs.get('merge', fallback='false').lower() == 'true'
        
        self.format = configs.get('format', fallback='%Y%m%d_%H%M%S')
        self.interval = int(configs.get('interval', fallback='24'))
        self.index_column = configs.get('index_column', fallback='time')
        self.index_unix = configs.get('index_unix', fallback='false').lower() == 'true'
        
        self.decimal = configs.get('decimal', fallback='.')
        self.separator = configs.get('separator', fallback=',')

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

    def _write_file(self, path, data):
        data = data.tz_convert(tz.utc).astype(float)
        data.index.name = 'time'
        
        if self.merge and os.path.isfile(path):
            csv = pd.read_csv(path, sep=self.separator, decimal=self.decimal, encoding='utf-8', index_col='time', parse_dates=['time'])
            if not csv.empty:
                csv.index = csv.index.tz_localize(tz.utc)
                
                if all(name in list(csv.columns) for name in list(data.columns)):
                    data = data.combine_first(csv)
                else:
                    data = pd.concat([csv, data], axis=1)
        
        data.to_csv(path, sep=self.separator, decimal=self.decimal, encoding='utf-8')

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

