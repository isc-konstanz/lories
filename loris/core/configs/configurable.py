# -*- coding: utf-8 -*-
"""
    loris.core.configs.configurable
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from collections.abc import Sequence, Mapping
from typing import List

import logging

from .configurations import Configurations

logger = logging.getLogger(__name__)


class Configurable:

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._configs = configs

    def __configure__(self, configs: Configurations) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(self)

    def __repr__(self) -> str:
        representation = f"{type(self).__name__}:\n"
        for attr in dir(self):
            if attr.startswith('_') or attr.isupper():
                continue
            try:
                value = getattr(self, attr)
                if value is None or callable(value) or isinstance(value, Configurations):
                    continue
                if isinstance(value, Configurable):
                    value = type(value).__name__
                representation += f"\t{attr} = {value}\n"
            except AttributeError:
                pass
        return representation + f"\tenabled = {self.is_enabled()}\n"

    # noinspection SpellCheckingInspection
    def configure(self) -> None:
        logger.debug(f"Configuring {type(self).__name__}: {self.configs.name}")
        self.__configure__(self._configs)

    @property
    def configs(self) -> Configurations:
        return self._configs

    def is_enabled(self) -> bool:
        return self._configs.enabled
