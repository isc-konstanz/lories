# -*- coding: utf-8 -*-
"""
    corsys.model
    ~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from abc import ABC

import logging
import pandas as pd
from .configs import Configurations, Configurable

logger = logging.getLogger(__name__)


class Model(ABC, Configurable):

    @classmethod
    def read(cls, system, conf_file: str = 'model.cfg') -> Model:
        return cls(Configurations.from_configs(system.configs, conf_file))

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)

    def __call__(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError
