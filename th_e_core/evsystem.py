# -*- coding: utf-8 -*-
"""
    th-e-core.pvsystem
    ~~~~~
    
    This module provides the :class:`th_e_core.PVSystem`, containing information about location, 
    orientation and datasheet parameters of a specific photovoltaic installation.
    
"""
import logging
logger = logging.getLogger(__name__)

from th_e_core.system import Component


class ElectricVehicle(Component):

    def __init__(self, configs, context, **kwargs):
        super().__init__(configs, context, **kwargs)

    def _configure(self, configs, **kwargs):
        #TODO: correct implementation missing
        self.name = 'ev'
        self.house_connection_point = configs.getfloat('General', 'house_connection_point', fallback=10)
        self.quantity = configs.getfloat('General', 'quantity', fallback=0)
        self.capacity = configs.getfloat('EV', 'capacity', fallback=30)
        self.driving_distance = configs.getfloat('EV', 'driving_distance', fallback=300)
        self.fuel_consumption = configs.getfloat('EV', 'fuel_consumption', fallback=18)
        self.charge_time = configs.get('EV', 'charging_time', fallback='18-6')
        self.charge_mode = configs.get('EV', 'duration', fallback='NORMAL')

    @property
    def type(self):
        return 'ev'

