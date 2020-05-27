# -*- coding: utf-8 -*-
"""
    th-e-core.pvsystem
    ~~~~~
    
    This module provides the :class:`th_e_core.PVSystem`, containing information about location, 
    orientation and datasheet parameters of a specific photovoltaic installation.
    
"""
import logging
logger = logging.getLogger(__name__)

import os
import pvlib as pv

from configparser import ConfigParser
from th_e_core.system import Component


class PVSystem(Component, pv.pvsystem.PVSystem):

    def __init__(self, configs, context, **kwargs):
        super().__init__(configs, context,
                         name = configs.get('General', 'id'), 
                         albedo = configs.getfloat('General', 'albedo'), 
                         surface_tilt = configs.getfloat('Geometry', 'tilt'), 
                         surface_azimuth = configs.getfloat('Geometry', 'azimuth'), 
                         modules_per_string = configs.getint('Modules', 'count'), 
                         strings_per_inverter = configs.getint('Inverter', 'strings'), 
                         **self._init_parameters(configs, context), 
                         **kwargs)

    def _init_parameters(self, configs, *_):
        with open(os.path.join(configs['General']['config_dir'], configs['General']['id']+'.d', 'module.cfg')) as f:
            module_file = '[Module]\n' + f.read()
        
        module_configs = ConfigParser()
        module_configs.optionxform = str
        module_configs.read_string(module_file)
        module = {}
        for key, value in module_configs.items('Module'):
            try:
                module[key] = float(value)
                
            except ValueError:
                module[key] = value
        
        inverter = {}
        if configs.has_section('Inverter') and 'pdc0' in module:
            total = configs.getfloat('Inverter', 'strings') \
                    *configs.getfloat('Modules', 'count') \
                    *configs.getfloat('General', 'count')
            
            inverter['pdc0'] = float(module['pdc0'])*total
        
        return { 'module_parameters': module,
                 'inverter_parameters': inverter }

    @property
    def type(self):
        return 'pv'

