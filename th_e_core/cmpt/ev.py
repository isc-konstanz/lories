# -*- coding: utf-8 -*-
"""
    th-e-core.cmpt.ev
    ~~~~~~~~~~~~~~~~~


"""
from configparser import ConfigParser as Configurations
from th_e_core.system import Component


class ElectricVehicle(Component):

    def _configure(self, configs: Configurations) -> None:
        super()._configure(configs)
        self.capacity = configs.getfloat('EV', 'capacity', fallback=30)

        # self.house_connection_point = configs.getfloat('General', 'house_connection_point', fallback=10)
        # self.quantity = configs.getfloat('General', 'quantity', fallback=0)
        #
        # self.driving_distance = configs.getfloat('EV', 'driving_distance', fallback=300)
        # self.fuel_consumption = configs.getfloat('EV', 'fuel_consumption', fallback=18)
        # self.charge_time = configs.get('EV', 'charging_time', fallback='18-6')
        # self.charge_mode = configs.get('EV', 'duration', fallback='NORMAL')

    @property
    def type(self) -> str:
        return 'ev'

    @property
    def capacity(self) -> float:
        return self._capacity
