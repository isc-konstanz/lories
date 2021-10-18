# -*- coding: utf-8 -*-
"""
    th-e-core.model
    ~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from abc import ABC, abstractmethod

import logging
import pandas as pd

from configparser import ConfigParser as Configurations
from th_e_core.configs import Configurable
from th_e_core.system import System

logger = logging.getLogger(__name__)


class Model(ABC, Configurable):

    @classmethod
    def read(cls, system: System, **kwargs) -> Model:
        return cls(system, cls._read_configs(system, **kwargs), **kwargs)

    @staticmethod
    def _read_configs(system: System, config_name: str = 'model.cfg', **kwargs) -> Configurations:
        return Configurable._read_configs(system.configs.get('General', 'root_dir'),
                                          system.configs.get('General', 'lib_dir'),
                                          system.configs.get('General', 'tmp_dir'),
                                          system.configs.get('General', 'data_dir'),
                                          system.configs.get('General', 'config_dir'),
                                          config_name, **kwargs)

    def __init__(self, system: System, configs: Configurations, **kwargs) -> None:
        super().__init__(configs, **kwargs)

        self._system = system
        self._build(system, configs, **kwargs)

    def _build(self, system, configs, **kwargs) -> None:
        pass

    @abstractmethod
    def run(self, *args, **kwargs) -> pd.DataFrame:
        pass

