# -*- coding: utf-8 -*-
"""
    corsys.cmpt.pv
    ~~~~~~~~~~~~~~
    
    This module provides the :class:`corsys.Photovoltaics`, containing information about orientation
    and datasheet parameters of a specific photovoltaic installation.
    
"""
from ..configs import Configurations
from . import Component


class Photovoltaic(Component):

    TYPE = 'pv'

    POWER = 'pv_power'
    POWER_EXP = 'pv_exp_power'

    ENERGY = 'pv_energy'
    ENERGY_EXP = 'pv_exp_energy'

    # noinspection PyProtectedMember
    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)

        self._power_max = configs.getfloat('General', 'power_max', fallback=None)
        if self._power_max:
            self._power_max *= 1000.

    @property
    def type(self) -> str:
        return self.TYPE

    @property
    def power_max(self) -> float:
        return self._power_max
