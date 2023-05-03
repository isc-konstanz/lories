# -*- coding: utf-8 -*-
"""
    corsys.cmpt.ees
    ~~~~~~~~~~~~~~~


"""
import pandas as pd

from ..configs import Configurations
from . import Component


class ElectricalEnergyStorage(Component):

    TYPE = 'ees'

    STATE_OF_CHARGE = 'ees_soc'

    POWER_CHARGE = 'ees_charge_power'
    POWER_DISCHARGE = 'ees_discharge_power'

    ENERGY_CHARGE = 'ees_charge_energy'
    ENERGY_DISCHARGE = 'ees_discharge_energy'

    # noinspection PyProtectedMember
    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        self._capacity = configs.getfloat(Configurations.GENERAL, 'capacity')
        self._efficiency = configs.getfloat(Configurations.GENERAL, 'efficiency')

        self._power_max = configs.getfloat(Configurations.GENERAL, 'power_max') * 1000.

    @property
    def type(self) -> str:
        return self.TYPE

    @property
    def capacity(self) -> float:
        return self._capacity

    def percent_to_energy(self, percent) -> float:
        return percent * self._capacity / 100

    def energy_to_percent(self, capacity) -> float:
        return capacity / self._capacity * 100

    @property
    def efficiency(self) -> float:
        return self._efficiency

    @property
    def power_max(self) -> float:
        return self._power_max
