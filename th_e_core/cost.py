# -*- coding: utf-8 -*-
"""
    th-e-core.cost
    ~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

from configparser import ConfigParser as Configurations
from th_e_core.configs import Configurable, ConfigurationException, ConfigurationUnavailableException
from th_e_core.system import System


class Cost(Configurable):

    def __init__(self, system: System, configs: Configurations) -> None:
        Configurable.__init__(self, configs)
        self._system = system
        self._build(system, configs)

    def _build(self, system: System, configs: Configurations) -> None:
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
