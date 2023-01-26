# -*- coding: utf-8 -*-
"""
    th-e-core.cost
    ~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

from .configs import Configurations, Configurable, ConfigurationException, ConfigurationUnavailableException
from .system import System


class Cost(Configurable):

    def __init__(self, system: System, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self._context = system
        self.__build__(system, configs)

    def __build__(self, system: System, configs: Configurations) -> None:
        pass


class CostException(ConfigurationException):
    """
    Raise if a cost section is invalid.

    """
    pass


class CostUnavailableException(ConfigurationUnavailableException):
    """
    Raise if a cost section can not be found.

    """
    pass
