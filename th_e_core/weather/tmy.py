# -*- coding: utf-8 -*-
"""
    th-e-core.weather.tmy
    ~~~~~~~~~~~~~~~~~~~~~

    
"""
from __future__ import annotations

import os
import pandas as pd

from pvlib.iotools import read_tmy2, read_tmy3
from ..configs import Configurations
from ..system import System
from .wx import Weather


class TMYWeather(Weather):

    def __configure__(self, configs: Configurations, **_) -> None:
        self.version = int(configs.get('General', 'version', fallback='3'))

        if 'file' in configs['TMY'] and not os.path.isabs(configs['TMY']['file']):
            configs['TMY']['file'] = os.path.join(configs.dirs.data,
                                                  configs['TMY']['file'])

        self.file = configs.get('TMY', 'file', fallback=None)
        self.year = configs.getint('TMY', 'year', fallback=None)

    # noinspection PyShadowingBuiltins
    def __activate__(self, system: System, **kwargs) -> None:
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
