# -*- coding: utf-8 -*-
"""
    th-e-core.model
    ~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from abc import ABC

import logging
import pandas as pd
from . import Configurations, Configurable, System

logger = logging.getLogger(__name__)


class Model(Configurable, ABC):

    @classmethod
    def read(cls, system: System) -> Model:
        return cls(system, cls._read_configs(system))

    @staticmethod
    def _read_configs(system: System, config_file: str = 'model.cfg') -> Configurations:
        return Configurable._read_configs(system.configs.get('General', 'root_dir'),
                                          system.configs.get('General', 'lib_dir'),
                                          system.configs.get('General', 'tmp_dir'),
                                          system.configs.get('General', 'data_dir'),
                                          system.configs.get('General', 'config_dir'),
                                          config_file)

    def __init__(self, system: System, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self._context = system
        self.__build__(system, configs)

    def __build__(self, system: System, configs: Configurations) -> None:
        pass

    def __call__(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError
