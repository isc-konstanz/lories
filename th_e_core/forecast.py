# -*- coding: utf-8 -*-
"""
    th-e-core.forecast
    ~~~~~
    
    This module provides the :class:`pvsyst.Weather`, used as reference to calculate a 
    photovoltaic installations' generated power. The provided environmental data contains 
    temperatures and horizontal solar irradiation, which can be used, to calculate the 
    effective irradiance on defined, tilted photovoltaic systems.
    
"""
import logging
logger = logging.getLogger(__name__)

import os
import datetime as dt
import pandas as pd
from io import StringIO
from abc import ABC, abstractmethod

from configparser import ConfigParser
from th_e_core.configs import Configurable
from th_e_core.database import Database
from th_e_core.weather import Weather


class Forecast(ABC, Configurable):

    @classmethod
    def read(cls, context, **kwargs):
        if not isinstance(context, Configurable):
            raise TypeError('Invalid context type: {}'.format(type(context)))
        
        configs = cls.read_configs(context, **kwargs)
        return cls.from_configs(context, configs, **kwargs)

    @staticmethod
    def read_configs(context, config_name='forecast.cfg', **kwargs):
        if not isinstance(context, Configurable):
            raise TypeError('Invalid context type: {}'.format(type(context)))
        
        return Configurable._read_configs(context._configs.get('General', 'root_dir'), 
                                          context._configs.get('General', 'lib_dir'), 
                                          context._configs.get('General', 'data_dir'), 
                                          context._configs.get('General', 'config_dir'), 
                                          config_name, **kwargs)

    @staticmethod
    def from_configs(context, configs, **kwargs):
        package = context._configs.get('Import', 'package', fallback='.'.join(context.__module__.split('.')[:-1]))
        forecast = Forecast._from_configs(configs, package, 'forecast', 'Forecast', 
                                          context, **kwargs)
        
        if not isinstance(forecast, Forecast):
            raise TypeError('Invalid forecast type: {}'.format(type(forecast)))
        
        return forecast

    def __init__(self, configs, context, **kwargs):
        if not isinstance(configs, ConfigParser):
            raise ValueError('Invalid configuration type: {}'.format(type(configs)))
        
        if not configs.has_option('General', 'id'):
            if configs.getboolean('General', 'central', fallback=True):
                if context is None:
                    raise ValueError('Invalid configuration, missing specified forecast id')
            
                configs.set('General', 'id', 
                            '{0:06.2f}'.format(float(context._location.latitude)).replace('.', '') + '_' + \
                            '{0:06.2f}'.format(float(context._location.longitude)).replace('.', ''))
        
        self._id = configs.get('General', 'id', fallback='')
        
        self._context = context
        self._configs = configs
        self._configure(configs, **kwargs)
        self._activate(context, **kwargs)

    def _configure(self, configs, **kwargs): #@UnusedVariable
        self.interval = configs.getint('General', 'interval', fallback=1440)*3600
        self.delay = configs.getint('General', 'delay', fallback=0)*3600

    def _activate(self, context, **kwargs): #@UnusedVariable
        configs = self._configs
        if configs.has_section('Database'):
            if configs.getboolean('General', 'central', fallback=True):
                data_dir = configs['General']['lib_dir']
            else:
                data_dir = configs['General']['data_dir']
            
            if 'dir' in configs['Database']:
                database_dir = configs['Database']['dir']
                if not os.path.isabs(database_dir):
                    configs['Database']['dir'] = os.path.join(data_dir, database_dir)
            else:
                configs['Database']['dir'] = data_dir
            
            self._database = Database.open(configs, **kwargs)
        else:
            self._database = None

    def _rename(self, data, variables=None):
        """
        Renames the columns according the variable mapping.

        Parameters
        ----------
        data: DataFrame
        variables: None or dict, default None
            If None, uses self.variables

        Returns
        -------
        data: DataFrame
            Renamed data.
        """
        if variables is None:
            variables = self.variables
        return data.rename(columns={y: x for x, y in variables.items()})

    def get(self, time=dt.datetime.now(), **kwargs):
        """ 
        Retrieves the forecasted data for a specified time interval
        
        :param time: 
            the time for which forecasted data will be looked up for.
            For many applications, passing datetime.datetime.now() will suffice.
        :type time: 
            :class:`pandas.tslib.Timestamp` or datetime
        
        :returns: 
            the forecasted data, indexed in a specific time interval.
        
        :rtype: 
            :class:`pandas.DataFrame`
        """
        if self._database is not None and self._database.exists(time, subdir=self._id):
            forecast = self._database.get(time, subdir=self._id)
        
        else:
            forecast = self._get(time, **kwargs)
            
            if self._database is not None:
                # Store the retrieved forecast
                self._database.persist(forecast, subdir=self._id)
        
        return forecast.loc[time:, :]

    @abstractmethod
    def _get(self, *args, **kwargs):
        pass


class NMM(Forecast, Weather):
    """
    Subclass of the Forecast class representing the Meteoblue
    NMM (Nonhydrostatic Meso-Scale Modelling) weather forecast model.
    
    Model data corresponds to 4km resolution forecasts.
    """

    def _configure(self, configs, **kwargs):
        super()._configure(configs, **kwargs);
        
        # TODO: add validity verification
        self.name = configs.get('Meteoblue', 'name')
        self.address = configs.get('Meteoblue', 'address')
        self.apikey = configs.get('Meteoblue', 'apikey')
        
        self.variables = {
            'sunshine':                     'sunshinetime',
            'daylight':                     'isdaylight',
            'temp_air':                     'temperature',
            'temp_felt':                    'felttemperature',
            'wind_speed':                   'windspeed',
            'wind_direction':               'winddirection',
            'humidity_rel':                 'relativehumidity',
            'pressure_sea':                 'sealevelpressure',
            'precipitation_convective':     'convective_precipitation',
            'precipitation_probability':    'precipitation_probability',
            'snow_fraction':                'snowfraction',
            'low_clouds':                   'lowclouds',
            'mid_clouds':                   'midclouds',
            'high_clouds':                  'highclouds',
            'total_clouds':                 'totalcloudcover',
            'uv_index':                     'uvindex',
            'gni':                          'gni_backwards',
            'dni':                          'dni_backwards',
            'ghi':                          'ghi_backwards',
            'dhi':                          'dif_backwards',
            'dhi_instant':                  'dif_instant',
            'etr':                          'extraterrestrialradiation_backwards',
            'etr_instant':                  'extraterrestrialradiation_instant'
        }
        
        self.variables_output = [
            'sunshine',                     # Sonnenscheindauer [min]
            'daylight',                     # Tageslicht Indikator
            'temp_air',                     # Temperatur [°C]
            'temp_felt',                    # Gefühlte Temperatur [°C]
            'wind_speed',                   # Windgeschwindigkeit [m/s]
            'wind_direction',               # Windrichtung [°]
            'gni',                          # Average Global Normal Irradiance of the last interval [W/m^2]
            'dni',                          # Average Direct Normal Irradiance of the last interval [W/m^2]
            'ghi',                          # Average Global Horizontal Irradiance of the last interval [W/m^2]
            'dhi',                          # Average Diffuse Horizontal Irradiance of the last interval [W/m^2]
            'etr',                          # Average Extraterrestrial Solar Radiation of the last interval [W/m^2]
            'dni_instant',                  # Global Normal Irradiance at the exact time [W/m^2]
            'gni_instant',                  # Global Normal Irradiance at the exact time [W/m^2]
            'ghi_instant',                  # Global Horizontal Irradiance at the exact time [W/m^2]
            'dhi_instant',                  # Diffuse Horizontal Irradiance at the exact time [W/m^2]
            'etr_instant',                  # Extraterrestrial Solar Radiation at the exact time [W/m^2]
            'total_clouds',                 # Gesamtbedeckungsgrad mit Wolken [%]
            'low_clouds',                   # Bedeckungsgrad mit niedrigen Wolken [%]
            'mid_clouds',                   # Bedeckungsgrad mit mittleren Wolken [%]
            'high_clouds',                  # Bedeckungsgrad mit hohen Wolken [%]
            'visibility',                   # Sichtweite [km]
            'uv_index',                     # UV index numbers from 1 to 16
            'humidity_rel',                 # Relative luftfeuchtigkeit [%]
            'pressure_sea',                 # Luftdruck auf Hoehe des Meerespiegels [hPa]
            'precipitation',                # Niederschlagsmenge [mm]
            'precipitation_convective',     # Niederschlag als Schauer [mm]
            'precipitation_probability',    # Niederschlagswahrscheinlichkeit [%]
            'snow_fraction'                 # Schneefall [0.0 - 1.0]
        ]

    def _activate(self, context, **kwargs):
        super()._activate(context, **kwargs)
        from pvlib.location import Location
        if not hasattr(context, '_location') or not isinstance(context._location, Location):
            raise ValueError("Invalid forecast context missing location information")
        
        self.location = context._location

    def get_meta(self):
        import requests
        import json
        
        parameters = {
            'name': self.name,
            'tz': self.location.tz,
            'lat': self.location.latitude,
            'lon': self.location.longitude,
            'asl': self.location.altitude,
            'timeformat': 'iso8601',
            'format': 'json',
            'apikey': self.apikey
        }
        response = requests.get(self.address + 'packages/basic-1h_clouds-1h_solar-1h', params=parameters)
        
        if response.status_code != 200:
            raise requests.HTTPError("Response returned with error " + response.status_code + ": " + response.reason)
        
        data = json.loads(response.text)
        return data.get('metadata')

    def _get(self, *_):
        data = self._rename(self._get_data())
        return data[self.variables_output]

    def _get_data(self):
        """
        Submits a query to the meteoblue servers and
        converts the CSV response to a pandas DataFrame.
        
        Returns
        -------
        data : DataFrame
            column names are the weather model's variable names.
        """
        import requests
        
        parameters = {
            'name': self.name,
            'tz': self.location.tz,
            'lat': self.location.latitude,
            'lon': self.location.longitude,
            'asl': self.location.altitude,
            'temperature': 'C',
            'windspeed': 'ms-1',
            'winddirection': 'degree',
            'precipitationamount': 'mm',
            'timeformat': 'iso8601',
            'format': 'csv',
            'apikey': self.apikey
        }
        response = requests.get(self.address + 'packages/basic-1h_clouds-1h_solar-1h', params=parameters)
        
        if response.status_code != 200:
            raise requests.HTTPError("Response returned with error " + response.status_code + ": " + response.reason)
        
        data = pd.read_csv(StringIO(response.text), sep=',')
        data['time'] = pd.to_datetime(data['time'])
        data = data.set_index('time')
        data = data.tz_convert(self.location.tz)
        data = data.replace(-999, 0)
        
        return data

