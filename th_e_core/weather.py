# -*- coding: utf-8 -*-
"""
    th-e-core.weather
    ~~~~~
    
    This module provides the :class:`pvsyst.Weather`, used as reference to calculate a 
    photovoltaic installations' generated power. The provided environmental data contains 
    temperatures and horizontal solar irradiation, which can be used, to calculate the 
    effective irradiance on defined, tilted photovoltaic systems.
    
"""
import logging
logger = logging.getLogger(__name__)

import os
import pytz as tz
import datetime as dt
from abc import ABC, abstractmethod

from th_e_core.configs import Configurable
from th_e_core.database import Database
from th_e_core.system import System


class Weather(ABC, Configurable):

    @classmethod
    def read(cls, context, **kwargs):
        if not isinstance(context, Configurable):
            raise TypeError('Invalid context type: {}'.format(type(context)))
        
        configs = cls.read_configs(context, **kwargs)
        return cls.from_configs(context, configs, **kwargs)

    @staticmethod
    def read_configs(context, config_name='weather.cfg', **kwargs):
        if not isinstance(context, Configurable):
            raise TypeError('Invalid context type: {}'.format(type(context)))
        
        return Configurable._read_configs(context._configs.get('General', 'root_dir'), 
                                          context._configs.get('General', 'lib_dir'), 
                                          context._configs.get('General', 'tmp_dir'), 
                                          context._configs.get('General', 'data_dir'), 
                                          context._configs.get('General', 'config_dir'), 
                                          config_name, **kwargs)

    @staticmethod
    def from_configs(context, configs, **kwargs):
        type = configs.get('General', 'type', fallback='default') #@ReservedAssignment
        if type.lower() in ['default', 'database']:
            return DatabaseWeather(configs, context, **kwargs)
        elif type.lower() == 'tmy':
            return TMYWeather(configs, context, **kwargs)
        elif type.lower() == 'epw':
            return EPWWeather(configs, context, **kwargs)
        
        package = context._configs.get('Import', 'package', fallback='.'.join(context.__module__.split('.')[:-1]))
        weather = Weather._from_configs(configs, package, 'weather', 'Weather', 
                                        context, **kwargs)
        
        if not isinstance(weather, Weather):
            raise TypeError('Invalid weather type: {}'.format(type(weather)))
        
        return weather

    def __init__(self, configs, context, **kwargs):
        super().__init__(configs, **kwargs)
        
        self._context = context
        self._activate(context, **kwargs)

    def _activate(self, context, **kwargs):
        pass

    @property
    def _system(self):
        if not isinstance(self._context, System):
            raise TypeError('Context is not of type System: {}'.format(type(self._context)))
        
        return self._context

    @abstractmethod
    def get(self, **kwargs):
        pass


class DatabaseWeather(Weather):

    def _activate(self, context, **kwargs):
        super()._activate(context, **kwargs)
        self._database = Database.open(self._configs, **kwargs)

    def get(self, start=None, end=None, format='%d.%m.%Y', **kwargs): #@ReservedAssignment
        if start is None:
            start = tz.utc.localize(dt.datetime.utcnow())
            start.replace(year=start.year-1, month=1, day=1, hour=0, minute=0, second=0)
        elif isinstance(start, str):
            start = tz.utc.localize(dt.datetime.strptime(start, format))
        
        if end is None:
            end = start + dt.timedelta(days=364)
        elif isinstance(end, str):
            end = tz.utc.localize(dt.datetime.strptime(end, format))
        
        return self._database.get(start=start, end=end, **kwargs)


class TMYWeather(Weather):

    def _configure(self, configs, **_):
        self.version = int(configs.get('General', 'version', fallback='3'))
        
        if 'file' in configs['TMY'] and not os.path.isabs(configs['TMY']['file']):
            configs['TMY']['file'] = os.path.join(configs['General']['data_dir'], 
                                                  configs['TMY']['file'])
        
        self.file = configs.get('TMY', 'file', fallback=None)
        self.year = configs.get('TMY', 'year', fallback=None)

    def _activate(self, system, **kwargs): #@UnusedVariable
        from pvlib.iotools import read_tmy2, read_tmy3
        
        if self.version == 3:
            self.data, self.meta = read_tmy3(filename=self.file, coerce_year=self.year)
            
        elif self.version == 2:
            self.data, self.meta = read_tmy2(self.file)
        else:
            raise ValueError('Invalid TMY version: {}'.format(self.version))

    def get(self, **_):
        # TODO: implement optional slicing
        return self.data


class EPWWeather(Weather):

    def _configure(self, configs, **_):
        if 'file' in configs['EPW'] and not os.path.isabs(configs['EPW']['file']):
            configs['EPW']['file'] = os.path.join(configs['General']['data_dir'], 
                                                  configs['EPW']['file'])
        
        self.file = configs.get('EPW', 'file', fallback=None)
        self.year = configs.get('EPW', 'year', fallback=None)

    def _activate(self, system, **kwargs): #@UnusedVariable
        from pvlib.iotools import read_epw
        self.data, self.meta = read_epw(filename=self.file, coerce_year=self.year)

    def get(self, **_):
        # TODO: implement optional slicing
        return self.data

