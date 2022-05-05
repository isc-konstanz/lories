# -*- coding: utf-8 -*-
"""
    th-e-core.model
    ~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from abc import ABC

import logging

from configparser import ConfigParser as Configurations
from th_e_core.configs import Configurable
from th_e_core.system import System

logger = logging.getLogger(__name__)


class Model(ABC, Configurable):

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

    def __init__(self, system: System, configs: Configurations) -> None:
        Configurable.__init__(self, configs)
        self._system = system
        self._build(system, configs)

    def _build(self, system: System, configs: Configurations) -> None:
        pass
