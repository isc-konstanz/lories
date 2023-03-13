# -*- coding: utf-8 -*-
"""
    corsys.model
    ~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from abc import ABC

import logging
import pandas as pd
from .cmpt import Context
from .configs import Configurations, Configurable

logger = logging.getLogger(__name__)


class Model(ABC, Configurable):

    @classmethod
    def read(cls, context: Context, conf_file: str = 'model.cfg') -> Model:
        return cls(context, Configurations.from_configs(context.configs, conf_file))

    def __init__(self, context: Context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self._context = context
        self.__build__(context, configs)

    def __build__(self, context: Context, configs: Configurations) -> None:
        pass

    def __call__(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError

    @property
    def context(self) -> Context:
        return self._context
