# -*- coding: utf-8 -*-
"""
    th-e-core.weather.epw
    ~~~~~~~~~~~~~~~~~~~~~
    
"""
from __future__ import annotations

import os
import re
import numpy as np
import pandas as pd
import logging

from pvlib.iotools import read_epw
from ..configs import Configurations
from ..system import System
from .wx import Weather

logger = logging.getLogger(__name__)


class EPWWeather(Weather):

    def __configure__(self, configs: Configurations, **_) -> None:
        if 'file' in configs['EPW'] and not os.path.isabs(configs['EPW']['file']):
            configs['EPW']['file'] = os.path.join(configs.dirs.data,
                                                  configs['EPW']['file'])

        self.file = configs.get('EPW', 'file', fallback=None)
        self.year = configs.getint('EPW', 'year', fallback=None)

    # noinspection PyShadowingBuiltins
    def __activate__(self, system: System, **kwargs) -> None:
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
