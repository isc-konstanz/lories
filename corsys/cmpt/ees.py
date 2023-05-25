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
        self.capacity = configs.getfloat(Configurations.GENERAL, 'capacity')
        self.efficiency = configs.getfloat(Configurations.GENERAL, 'efficiency')

        self.power_max = configs.getfloat(Configurations.GENERAL, 'power_max') * 1000

    @property
    def type(self) -> str:
        return self.TYPE

    def percent_to_energy(self, percent) -> float:
        return percent * self.capacity / 100

    def energy_to_percent(self, capacity) -> float:
        return capacity / self.capacity * 100

    def infer_soc(self, data: pd.DataFrame) -> pd.DataFrame:
        from copy import deepcopy
        from .. import System

        if System.POWER_EL not in data.columns:
            raise ValueError("Unable to infer battery storage state of charge without import/export power")

        columns = [self.STATE_OF_CHARGE, self.POWER_CHARGE, self.POWER_DISCHARGE]

        data = deepcopy(data)
        data.loc[data.index[0], columns] = [0, 0, 0]

        for i in range(1, len(data.index) - 1):
            index = data.index[i]
            hours = (index - data.index[i-1]).total_seconds() / 3600.

            power = -min(self.power_max, max(-self.power_max, data.loc[index, System.POWER_EL]))

            soc = data.loc[data.index[i-1], self.STATE_OF_CHARGE]
            charge_max = self.percent_to_energy(100 - soc)
            discharge_max = self.percent_to_energy(0 - soc)

            energy = power/1000. * hours
            energy = min(charge_max, max(discharge_max, energy))
            soc += self.energy_to_percent(energy)

            power = energy*1000. / hours
            data.loc[index, columns] = [soc, max(0., power), max(0., -power)]
            data.loc[index, System.POWER_EL] += power
        data.loc[abs(data[System.POWER_EL]) <= 1e-3, System.POWER_EL] = 0

        return data
