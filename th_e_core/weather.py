# -*- coding: utf-8 -*-
"""
    th-e-core.weather
    ~~~~~~~~~~~~~~~~~
    
    This module provides the :class:`th_e_core.Weather`, used as reference to calculate e.g.
    photovoltaic installations' generated power. The provided environmental data contains 
    temperatures and horizontal solar irradiation, which can be used, to calculate the 
    effective irradiance on defined, tilted photovoltaic systems.
    
"""
from __future__ import annotations
from abc import ABC, abstractmethod

import os
import re
import pytz as tz
import datetime as dt
import numpy as np
import pandas as pd
import logging

from .configs import Configurations, Configurable
from .system import System
from .io import Database

logger = logging.getLogger(__name__)


class Weather(ABC, Configurable):

    # noinspection PyShadowingBuiltins
    @classmethod
    def read(cls, system: System, **kwargs) -> Weather:
        configs = cls._read_configs(system, **kwargs)
        type = configs.get('General', 'type', fallback='default')
        if type.lower() in ['default', 'database']:
            return DatabaseWeather(system, configs, **kwargs)
        elif type.lower() == 'tmy':
            return TMYWeather(system, configs, **kwargs)
        elif type.lower() == 'epw':
            return EPWWeather(system, configs, **kwargs)

        raise TypeError('Invalid weather type: {}'.format(type))

    @staticmethod
    def _read_configs(system: System, config_file: str = 'weather.cfg', **kwargs) -> Configurations:
        return Configurable._read_configs(system.configs.get('General', 'root_dir'),
                                          system.configs.get('General', 'lib_dir'),
                                          system.configs.get('General', 'tmp_dir'),
                                          system.configs.get('General', 'data_dir'),
                                          system.configs.get('General', 'config_dir'),
                                          config_file, **kwargs)

    def __init__(self, system: System, configs: Configurations, **kwargs) -> None:
        super().__init__(configs, **kwargs)
        self._context = system
        self.__activate__(system, **kwargs)

    def __activate__(self, system: System, **kwargs) -> None:
        pass

    @abstractmethod
    def get(self, *args, **kwargs) -> pd.DataFrame:
        pass


class DatabaseWeather(Weather):

    def __activate__(self, system: System, **kwargs) -> None:
        super().__activate__(system, **kwargs)
        self._database = Database.open(self._configs, **kwargs)

    # noinspection PyShadowingBuiltins
    def get(self,
            start:  pd.Timestamp | dt.datetime | str = None,
            end:    pd.Timestamp | dt.datetime | str = None,
            format: str = '%d.%m.%Y',
            **kwargs) -> pd.DataFrame:

        if isinstance(start, str):
            start = tz.utc.localize(dt.datetime.strptime(start, format))

        if isinstance(end, str):
            end = tz.utc.localize(dt.datetime.strptime(end, format))

        return self._database.read(start=start, end=end, **kwargs)


class TMYWeather(Weather):

    def __configure__(self, configs: Configurations, **_) -> None:
        self.version = int(configs.get('General', 'version', fallback='3'))

        if 'file' in configs['TMY'] and not os.path.isabs(configs['TMY']['file']):
            configs['TMY']['file'] = os.path.join(configs['General']['data_dir'], 
                                                  configs['TMY']['file'])

        self.file = configs.get('TMY', 'file', fallback=None)
        self.year = configs.getint('TMY', 'year', fallback=None)

    # noinspection PyShadowingBuiltins
    def __activate__(self, system: System, **kwargs) -> None:
        from pvlib.iotools import read_tmy2, read_tmy3

        dir = os.path.dirname(self.file)
        if not os.path.isdir(dir):
            os.makedirs(dir, exist_ok=True)

        if self.version == 3:
            self.data, self.meta = read_tmy3(filename=self.file, coerce_year=self.year)

        elif self.version == 2:
            self.data, self.meta = read_tmy2(self.file)
        else:
            raise ValueError('Invalid TMY version: {}'.format(self.version))

    def get(self, **_) -> pd.DataFrame:
        # TODO: implement optional slicing
        return self.data


class EPWWeather(Weather):

    def __configure__(self, configs: Configurations, **_) -> None:
        if 'file' in configs['EPW'] and not os.path.isabs(configs['EPW']['file']):
            configs['EPW']['file'] = os.path.join(configs['General']['data_dir'], 
                                                  configs['EPW']['file'])

        self.file = configs.get('EPW', 'file', fallback=None)
        self.year = configs.getint('EPW', 'year', fallback=None)

    # noinspection PyShadowingBuiltins
    def __activate__(self, system: System, **kwargs) -> None:
        from pvlib.iotools import read_epw

        dir = os.path.dirname(self.file)
        if not os.path.isfile(self.file):
            os.makedirs(dir, exist_ok=True)
            self._download(system)

        self.data, self.meta = read_epw(filename=self.file, coerce_year=self.year)

    # noinspection PyPackageRequirements
    def _download(self, system: System) -> None:
        import requests
        import urllib3
        from urllib3.exceptions import InsecureRequestWarning
        urllib3.disable_warnings(InsecureRequestWarning)

        headers = {
            'User-Agent': "Magic Browser",
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }

        response = requests.get('https://github.com/NREL/EnergyPlus/raw/develop/weather/master.geojson', verify=False)
        data = response.json()  # metadata for available files
        # download lat/lon and url details for each .epw file into a dataframe

        locations = pd.DataFrame({'url': [], 'lat': [], 'lon': [], 'name': []})
        for location in data['features']:
            match = re.search(r'href=[\'"]?([^\'" >]+)', location['properties']['epw'])
            if match:
                url = match.group(1)
                name = url[url.rfind('/') + 1:]
                lontemp = location['geometry']['coordinates'][0]
                lattemp = location['geometry']['coordinates'][1]
                locations = locations.append(pd.DataFrame({'url': [url],
                                                           'lat': [lattemp],
                                                           'lon': [lontemp],
                                                           'name': [name]}), ignore_index=True)

        errorvec = np.sqrt(np.square(locations.lat - system.location.latitude) +
                           np.square(locations.lon - system.location.longitude))
        index = errorvec.idxmin()
        url = locations['url'][index]
        # name = locations['name'][index]

        response = requests.get(url, verify=False, headers=headers)
        if response.ok:
            with open(self.file, 'wb') as file:
                file.write(response.text.encode('ascii', 'ignore'))
        else:
            logger.warning('Connection error status code: %s' % response.status_code)
            response.raise_for_status()

    def get(self, **_) -> pd.DataFrame:
        # TODO: implement optional slicing
        return self.data
