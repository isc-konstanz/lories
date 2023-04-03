# -*- coding: utf-8 -*-
"""
    corsys.weather.wx
    ~~~~~~~~~~~~~~~~~
    
    This module provides the :class:`corsys.weather.Weather`, used as reference to
    calculate e.g. photovoltaic installations generated power. The provided environmental data
    contains temperatures and horizontal solar irradiation, which can be used, to calculate the
    effective irradiance on defined, tilted photovoltaic systems.

"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional, Dict

import pandas as pd
import logging

from ..cmpt import Context
from ..configs import Configurations, Configurable
from ..io import DatabaseUnavailableException

logger = logging.getLogger(__name__)


class Weather(ABC, Configurable):

    GHI = 'ghi'
    DNI = 'dni'
    DHI = 'dhi'
    TEMP_AIR = 'temp_air'
    TEMP_FELT = 'temp_felt'
    TEMP_DEW_POINT = 'dew_point'
    HUMIDITY_REL = 'relative_humidity'
    PRESSURE_SEA = 'pressure_sea'
    WIND_SPEED = 'wind_speed'
    WIND_SPEED_GUST = 'wind_speed_gust'
    WIND_DIRECTION = 'wind_direction'
    WIND_DIRECTION_GUST = 'wind_direction_gust'
    CLOUD_COVER = 'cloud_cover'
    CLOUDS_LOW = 'clouds_low'
    CLOUDS_MID = 'clouds_mid'
    CLOUDS_HIGH = 'clouds_high'
    SUNSHINE = 'sunshine'
    VISIBILITY = 'visibility'
    PRECIPITATION = 'precipitation'
    PRECIPITATION_CONV = 'precipitation_convective'
    PRECIPITATION_PROB = 'precipitation_probability'
    PRECIPITABLE_WATER = 'precipitable_water'
    SNOW_FRACTION = 'snow_fraction'

    # noinspection PyShadowingBuiltins
    @classmethod
    def read(cls, context: Context, conf_file: str = 'weather.cfg') -> Weather:
        configs = Configurations.from_configs(context.configs, conf_file)
        type = configs.get('General', 'type', fallback='default').lower()
        if type in ['default', 'database']:
            from .db import DatabaseWeather
            return DatabaseWeather(context, configs)
        elif type == 'brightsky':
            from .dwd import Brightsky
            return Brightsky(context, configs)
        elif type == 'nmm':
            from .nmm import NMM
            return NMM(context, configs)

        raise TypeError('Invalid weather type: {}'.format(type))

    def __init__(self, context: Context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        if not hasattr(self, '_variables'):
            self._variables = {}
        self._context = context

    def __activate__(self, context: Context) -> None:
        pass

    def __build__(self, **kwargs) -> Optional[pd.DataFrame]:
        pass

    def _rename(self, data: pd.DataFrame, variables: Dict[str, str] = None) -> pd.DataFrame:
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
            variables = self._variables
        return data.rename(columns={y: x for x, y in variables.items()})

    @property
    def database(self):
        raise DatabaseUnavailableException(f"Weather \"{self.context.location.name}\" has no database configured")

    @property
    def context(self) -> Context:
        return self._context

    def activate(self) -> None:
        self.__activate__(self._context)

    def build(self, **kwargs) -> Optional[pd.DataFrame]:
        return self.__build__(**kwargs)

    @abstractmethod
    def get(self, *args, **kwargs) -> pd.DataFrame:
        pass


class WeatherException(Exception):
    """
    Raise if an error occurred accessing the weather.

    """
    pass


class WeatherUnavailableException(WeatherException):
    """
    Raise if a configured weather access can not be found.

    """
    pass
