# -*- coding: utf-8 -*-
"""
    corsys.cmpt.ees
    ~~~~~~~~~~~~~~~


"""
from ..configs import Configurations
from . import Component


class ElectricalEnergyStorage(Component):

    STATE_OF_CHARGE = 'ees_soc'

    POWER_CHARGE = 'ees_charge_power'

    ENERGY_CHARGE = 'ees_charge_energy'
    ENERGY_DISCHARGE = 'ees_discharge_energy'

    # noinspection PyProtectedMember
    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        self._capacity = configs.getfloat('General', 'capacity')

        self._power_max = configs.getfloat('General', 'power_max')

    @property
    def type(self) -> str:
        return 'ees'

    @property
    def capacity(self) -> float:
        return self._capacity

    @property
    def power_max(self) -> float:
        return self._power_max
