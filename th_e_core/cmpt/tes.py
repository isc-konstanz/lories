# -*- coding: utf-8 -*-
"""
    th-e-core.cmpt.tes
    ~~~~~~~~~~~~~~~~~~


"""
from configparser import ConfigParser as Configurations
from th_e_core.system import Component


class ThermalEnergyStorage(Component):

    TEMPERATURE = 'tes_temp'
    TEMPERATURE_HEATING = 'tes_ht_temp'
    TEMPERATURE_DOMESTIC = 'tes_dom_temp'

    def _configure(self, configs: Configurations) -> None:
        super()._configure(configs)
        self._volume = configs.getfloat('General', 'volume')

        # For the thermal storage capacity in kWh/K, it will be assumed to be filled with water,
        # resulting in a specific heat capacity of 4.184 J/g*K.
        # TODO: Make tank content and specific heat capacity configurable
        self._capacity = 4.184*self.volume/3600

    @property
    def type(self) -> str:
        return 'tes'

    @property
    def volume(self) -> float:
        return self._volume

    @property
    def capacity(self) -> float:
        return self._capacity
