# -*- coding: utf-8 -*-
"""
    th-e-core.cmpt.ees
    ~~~~~~~~~~~~~~~~~~


"""
from configparser import ConfigParser as Configurations
from th_e_core.system import Component


class ElectricalEnergyStorage(Component):

    STATE_OF_CHARGE = 'ees_soc'

    POWER_CHARGE = 'ees_charge_power'

    ENERGY_CHARGE = 'ees_charge_energy'
    ENERGY_DISCHARGE = 'ees_discharge_energy'

    def _configure(self, configs: Configurations) -> None:
        super()._configure(configs)
        self._capacity = configs.getfloat('General', 'capacity')

        self._power_max = configs.getfloat('General', 'power_max')

        # self.energy_price = configs.getfloat('Evaluation', 'energy_price', fallback=30)
        # self.feed_in_tariff_pv = configs.getfloat('Evaluation', 'feed_in_tariff_pv', fallback=0.075)
        # self.data_path = configs.get('Evaluation', 'data_path')

    @property
    def type(self) -> str:
        return 'ees'

    @property
    def capacity(self) -> float:
        return self._capacity

    @property
    def power_max(self) -> float:
        return self._power_max
