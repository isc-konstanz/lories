# -*- coding: utf-8 -*-
"""
    th-e-core.model
    ~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from abc import ABC

import logging
import pandas as pd
from .system import System
from .configs import Configurations, Configurable

logger = logging.getLogger(__name__)


class Model(ABC, Configurable):

    @classmethod
    def read(cls, system: System, conf_file: str = 'model.cfg') -> Model:
        return cls(system, Configurations.from_configs(system.configs, conf_file))

    def __init__(self, system: System, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self._context = system
        self.__build__(system, configs)

    def __build__(self, system: System, configs: Configurations) -> None:
        pass

    def __call__(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError

    @property
    def context(self) -> System:
        return self._context
