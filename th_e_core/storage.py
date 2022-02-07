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


class ElectricalEnergyStorage(Component):

    def __init__(self, configs, context, **kwargs):
        super().__init__(configs, context, name=configs.get('General', 'id'), **kwargs)

    def _configure(self, configs, **kwargs):
        self.name = 'ees'
        self.capacity = configs.getfloat('General', 'capacity', fallback=0)
        self.energy_price = configs.getfloat('Evaluation', 'energy_price', fallback=30)
        self.feed_in_tariff_pv = configs.getfloat('Evaluation', 'feed_in_tariff_pv', fallback=0.075)
        self.data_path = configs.get('Evaluation', 'data_path')

    @property
    def type(self):
        return 'ees'

