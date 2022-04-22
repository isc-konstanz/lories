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

from th_e_core.pv import ModuleDatabase, InverterDatabase
from th_e_core import System, Component, ConfigurationException
from configparser import ConfigParser as Configurations

logger = logging.getLogger(__name__)


class PVSystem(Component, pvlib.pvsystem.PVSystem):

    POWER = 'pv_power'
    POWER_EXP = 'pv_exp_power'

    ENERGY = 'pv_energy'
    ENERGY_EXP = 'pv_exp_energy'

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

        self.strings_per_inverter = configs.getint('Inverter', 'strings', fallback=1)
        self.modules_per_string = configs.getint('Module', 'count', fallback=1)
        self.modules_per_inverter = self.strings_per_inverter * self.modules_per_string

        self.module_type = configs.get('Module', 'construct_type', fallback=None)
        self.module_parameters = self._load_module(configs)

        self.inverter_parameters = self._load_inverter(configs)
        self.inverters_per_system = configs.getfloat('Inverter', 'count', fallback=1)

        # TODO: implement temperature model parameters via configurations
        temperature_model_parameters = None
        if temperature_model_parameters is None:
            self.temperature_model_parameters = \
                self._infer_temperature_model_params()
        else:
            self.temperature_model_parameters = temperature_model_parameters

    def _load_module(self, configs: Configurations):
        module = {}

        self._read_module_database(module)
        self._read_module_override(module)
        if len(module.keys()) < 1:
            raise ConfigurationException("Unable to find module parameters")

        if 'pdc0' not in module and all(p in module for p in ['I_mp_ref', 'V_mp_ref']):
            module['pdc0'] = module['I_mp_ref'] \
                           * module['V_mp_ref']

        return module

    def _read_module_database(self, module: dict) -> bool:
        if 'type' in self.configs['Module']:
            module_type = self.configs['Module']['type']
            modules = ModuleDatabase(self.configs)
            self._update_parameters(module, modules.read(module_type))
            logger.debug('Read module "%s" from database of %s', module_type, self.name)
            return True
        return False

    def _read_module_override(self, module: dict) -> bool:
        module_file = os.path.join(self.configs['General']['config_dir'],
                                   self.configs['General']['id'] + '.d', 'module.cfg')

        if os.path.exists(module_file):
            with open(module_file) as f:
                module_str = '[Module]\n' + f.read()

            module_configs = Configurations()
            module_configs.optionxform = str
            module_configs.read_string(module_str)
            self._update_parameters(module, module_configs['Module'])
            logger.debug('Read module override file of component %s: %s', self.name, module_file)
            return True
        return False

    def _load_inverter(self, configs: Configurations):
        inverter = {}

        self._read_inverter_database(inverter)
        self._read_inverter_override(inverter)

        if 'pdc0' not in inverter and 'pdc0' in self.module_parameters:
            inverter['pdc0'] = self.module_parameters['pdc0'] *\
                               self.modules_per_inverter

        if len(inverter.keys()) < 1:
            raise ConfigurationException("Unable to find inverter parameters")

        return inverter

    def _read_inverter_database(self, inverter: dict) -> bool:
        if 'type' in self.configs['Inverter']:
            inverter_type = self.configs['Inverter']['type']
            inverters = InverterDatabase(self.configs)
            self._update_parameters(inverter, inverters.read(inverter_type))
            logger.debug('Read inverter "%s" from database of %s', inverter_type, self.name)
            return True
        return False

    def _read_inverter_override(self, inverter: dict) -> bool:
        inverter_file = os.path.join(self.configs['General']['config_dir'],
                                     self.configs['General']['id'] + '.d', 'inverter.cfg')

        if os.path.exists(inverter_file):
            with open(inverter_file) as f:
                inverter_str = '[Inverter]\n' + f.read()

            inverter_configs = Configurations()
            inverter_configs.optionxform = str
            inverter_configs.read_string(inverter_str)
            self._update_parameters(inverter, inverter_configs['Inverter'])
            logger.debug('Read inverter override file of component %s: %s', self.name, inverter_file)
            return True
        return False

    @staticmethod
    def _update_parameters(parameters: dict, update: dict):
        for key, value in update.items():
            try:
                parameters[key] = float(value)
            except ValueError:
                parameters[key] = value

        return parameters

    @property
    def type(self) -> str:
        return 'pv'
