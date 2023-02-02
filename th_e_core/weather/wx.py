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
from typing import Dict

import pandas as pd
import logging

from ..configs import Configurations, Configurable
from ..system import System

logger = logging.getLogger(__name__)


class Weather(ABC, Configurable):

    # noinspection PyShadowingBuiltins
    @classmethod
    def read(cls, system: System, conf_file: str = 'weather.cfg') -> Weather:
        configs = Configurations.from_configs(system.configs, conf_file)
        type = configs.get('General', 'type', fallback='default').lower()
        if type in ['default', 'database']:
            from .db import DatabaseWeather
            return DatabaseWeather(system, configs)
        elif type == 'tmy':
            from .tmy import TMYWeather
            return TMYWeather(system, configs)
        elif type == 'epw':
            from .epw import EPWWeather
            return EPWWeather(system, configs)
        elif type == 'nmm':
            from .nmm import NMM
            return NMM(system, configs)

        raise TypeError('Invalid weather type: {}'.format(type))

    def __init__(self, system: System, configs: Configurations, **kwargs) -> None:
        super().__init__(configs, **kwargs)
        self._variables = {}
        self._context = system
        self.__activate__(system, configs)

    def __activate__(self, system: System, configs: Configurations) -> None:
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
    def context(self) -> System:
        return self._context

    @abstractmethod
    def get(self, *args, **kwargs) -> pd.DataFrame:
        pass
