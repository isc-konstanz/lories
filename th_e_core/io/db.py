# -*- coding: utf-8 -*-
"""
    th-e-core.io.db
    ~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from abc import ABC, abstractmethod

import pytz as tz
import datetime as dt
import pandas as pd

from ..configs import Configurations


class Database(ABC):

    @staticmethod
    def open(configs: Configurations, **kwargs) -> Database:
        dbargs = dict(configs.items('Database'))
        kwargs.update(dbargs)

        database_type = kwargs.pop('type').lower()
        if database_type == 'sql':
            database_tables = dict(configs.items('Tables'))
            from th_e_core.io.sql import SqlDatabase
            return SqlDatabase(**kwargs, tables=database_tables)

        elif database_type == 'oem':
            database_feeds = dict(configs.items('Feeds'))
            from th_e_core.io.oem import EmonDatabase
            return EmonDatabase(**kwargs, feeds=database_feeds)

        elif database_type == 'csv':
            from th_e_core.io.csv import CsvDatabase
            return CsvDatabase(**kwargs)
        else:
            raise ValueError('Invalid database type argument')

    def __init__(self,
                 enabled: str = 'true',
                 timezone: str | tz.BaseTzInfo = tz.UTC) -> None:

        self.enabled = enabled.lower() == 'true'
        if isinstance(timezone, str):
            timezone = tz.timezone(timezone)
        self.timezone = timezone

    @abstractmethod
    def exists(self,
               start: pd.Timestamp | dt.datetime = None,
               end:   pd.Timestamp | dt.datetime = None,
               **kwargs) -> bool:
        """
        Returns if data for a specified time interval of a set of data series exists

        :param start:
            the time from which on values will be looked up for.
        :type start:
            :class:`pandas.Timestamp` or datetime

        :param end:
            the time until which values will be looked up for.
        :type end:
            :class:`pandas.Timestamp` or datetime

        :returns:
            whether values do exist in a specific time interval.
        :rtype:bool
        """
        pass

    @abstractmethod
    def read(self,
             start: pd.Timestamp | dt.datetime = None,
             end:   pd.Timestamp | dt.datetime = None,
             **kwargs) -> pd.DataFrame:
        """ 
        Retrieve data for a specified time interval of a set of data series
        
        :param start: 
            the time from which on values will be looked up for.
        :type start: 
            :class:`pandas.Timestamp` or datetime
        
        :param end: 
            the time until which values will be looked up for.
        :type end: 
            :class:`pandas.Timestamp` or datetime
        
        :returns: 
            the retrieved values, indexed in a specific time interval.
        :rtype: 
            :class:`pandas.DataFrame`
        """
        pass

    @abstractmethod
    def write(self, data: pd.DataFrame, **kwargs) -> None:
        """ 
        Write a set of data values, to persistently store them
        
        :param data: 
            the data set to be written
        :type data: 
            :class:`pandas.DataFrame`
        """
        pass

    def close(self, **kwargs) -> None:
        """ 
        Closes the database and cleans up all resources
        
        """
        pass


class DatabaseException(Exception):
    """
    Raise if an error occurred accessing the database.

    """
    pass


class DatabaseUnavailableException(DatabaseException):
    """
    Raise if a configured database can not be found.

    """
    pass
