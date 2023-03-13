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
from typing import Dict

import pandas as pd
import logging

from ..cmpt import Context
from ..configs import Configurations, Configurable

logger = logging.getLogger(__name__)


class Weather(ABC, Configurable):

    # noinspection PyShadowingBuiltins
    @classmethod
    def read(cls, context: Context, conf_file: str = 'weather.cfg') -> Weather:
        configs = Configurations.from_configs(context.configs, conf_file)
        type = configs.get('General', 'type', fallback='default').lower()
        if type in ['default', 'database']:
            from .db import DatabaseWeather
            return DatabaseWeather(context, configs)
        elif type == 'tmy':
            from .tmy import TMYWeather
            return TMYWeather(context, configs)
        elif type == 'epw':
            from .epw import EPWWeather
            return EPWWeather(context, configs)
        elif type == 'nmm':
            from .nmm import NMM
            return NMM(context, configs)

        raise TypeError('Invalid weather type: {}'.format(type))

    def __init__(self, context: Context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self._context = context
        self.__activate__(context, configs)

    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        self._variables = {}

    def __activate__(self, context: Context, configs: Configurations) -> None:
        pass

    def build(self, **kwargs) -> pd.DataFrame:
        return self.__build__(**kwargs)

    def __build__(self, **kwargs) -> pd.DataFrame:
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
    def context(self) -> Context:
        return self._context

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
