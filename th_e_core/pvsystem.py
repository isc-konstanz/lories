# -*- coding: utf-8 -*-
"""
    th-e-core.pvsystem
    ~~~~~~~~~~~~~~~~~~
    
    This module provides the :class:`th_e_core.PVSystem`, containing information about location,
    orientation and datasheet parameters of a specific photovoltaic installation.
    
"""
import os
import logging
import pvlib

from th_e_core.pvtools import ModuleDatabase, InverterDatabase
from th_e_core.system import System, Component
from configparser import ConfigParser as Configurations

logger = logging.getLogger(__name__)


class PVSystem(Component, pvlib.pvsystem.PVSystem):

    def __init__(self, system: System, configs: Configurations, **kwargs) -> None:
        super().__init__(system, configs, name=configs.get('General', 'id'), **kwargs)

    def _configure(self, configs: Configurations, **kwargs) -> None:
        super()._configure(configs, **kwargs)

        self.racking_model = configs.get('Mounting', 'type', fallback=None)
        self.surface_tilt = configs.getfloat('Mounting', 'tilt', fallback=0)
        self.surface_azimuth = configs.getfloat('Mounting', 'azimuth', fallback=180)

        self.surface_type = configs.get('General', 'ground_type', fallback=None)
        if self.surface_type is not None:
            from pvlib import irradiance
            self.albedo = irradiance.SURFACE_ALBEDOS.get(self.surface_type, 0.25)
        else:
            self.albedo = configs.getfloat('General', 'albedo', fallback=0.25)

        self.module_type = configs.get('Module', 'construct_type', fallback=None)
        self.module_parameters = self._configure_module(configs)

        self.inverter_parameters = self._configure_inverter(configs)
        self.inverters_per_system = configs.getfloat('Inverter', 'count', fallback=1)

        self.strings_per_inverter = configs.getint('Inverter', 'strings', fallback=1)

        self.modules_per_string = configs.getint('Module', 'count', fallback=1)
        self.modules_per_inverter = self.strings_per_inverter * self.modules_per_string

        # TODO: implement temperature model parameters via configurations
        temperature_model_parameters = None
        if temperature_model_parameters is None:
            self.temperature_model_parameters = \
                self._infer_temperature_model_params()
        else:
            self.temperature_model_parameters = temperature_model_parameters

    def _configure_module(self, configs):
        module = {}

        if 'type' in configs['Module']:
            module_type = configs['Module']['type']
            modules = ModuleDatabase(configs)
            module = modules.read(module_type)

        def module_update(items):
            for key, value in items:
                try:
                    module[key] = float(value)
                except ValueError:
                    module[key] = value

        if configs.has_section('Parameters'):
            module_update(configs.items('Parameters'))

        module_file = os.path.join(configs['General']['config_dir'],
                                   configs['General']['id'] + '.d', 'module.cfg')

        if os.path.exists(module_file):
            with open(module_file) as f:
                module_str = '[Module]\n' + f.read()

            module_configs = Configurations()
            module_configs.optionxform = str
            module_configs.read_string(module_str)
            module_update(module_configs.items('Module'))

        if 'pdc0' not in module and all(p in module for p in ['I_mp_ref', 'V_mp_ref']):
            module['pdc0'] = module['I_mp_ref'] \
                           * module['V_mp_ref']

        return module

    def _configure_inverter(self, configs):
        inverter = {}

        if 'type' in configs['Inverter']:
            # TODO: Test and verify inverter CEC parameters
            inverter_type = configs['Inverter']['type']
            inverters = InverterDatabase(configs)
            inverter = inverters.read(inverter_type)

        def inverter_update(items):
            for key, value in items:
                try:
                    inverter[key] = float(value)
                except ValueError:
                    inverter[key] = value

        inverter_file = os.path.join(configs['General']['config_dir'],
                                     configs['General']['id'] + '.d', 'inverter.cfg')

        if os.path.exists(inverter_file):
            with open(inverter_file) as f:
                inverter_str = '[Inverter]\n' + f.read()

            inverter_configs = Configurations()
            inverter_configs.optionxform = str
            inverter_configs.read_string(inverter_str)
            inverter_update(inverter_configs.items('Inverter'))

        if 'pdc0' not in inverter and \
                configs.has_section('Inverter'):
            total = configs.getfloat('Inverter', 'strings') \
                    * configs.getfloat('Module', 'count')
            inverter['pdc0'] = self.module_parameters['pdc0'] * total

        return inverter

    @property
    def type(self) -> str:
        return 'pv'
