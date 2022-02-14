# -*- coding: utf-8 -*-
"""
    th-e-core.pvsystem
    ~~~~~
    
    This module provides the :class:`th_e_core.PVSystem`, containing information about location, 
    orientation and datasheet parameters of a specific photovoltaic installation.
    
"""
import logging

from configparser import ConfigParser as Configurations
from th_e_core import Component, System

logger = logging.getLogger(__name__)


class ElectricalEnergyStorage(Component):

    def __init__(self, system: System, configs: Configurations, **kwargs) -> None:
        super().__init__(system, configs, **kwargs)

    def _configure(self, configs, **kwargs):
        self.name = 'ees'
        self.capacity = configs.getfloat('General', 'capacity', fallback=0)
        self.energy_price = configs.getfloat('Evaluation', 'energy_price', fallback=30)
        self.feed_in_tariff_pv = configs.getfloat('Evaluation', 'feed_in_tariff_pv', fallback=0.075)
        self.data_path = configs.get('Evaluation', 'data_path')

    @property
    def type(self):
        return 'ees'
