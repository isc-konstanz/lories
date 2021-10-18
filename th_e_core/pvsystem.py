# -*- coding: utf-8 -*-
"""
    th-e-core.pvsystem
    ~~~~~~~~~~~~~~~~~~
    
    This module provides the :class:`th_e_core.PVSystem`, containing information about location,
    orientation and datasheet parameters of a specific photovoltaic installation.
    
"""
import logging
import pvlib as pv

from configparser import ConfigParser as Configurations
from th_e_core.system import System, Component

logger = logging.getLogger(__name__)


class PVSystem(Component, pv.pvsystem.PVSystem):

    def __init__(self, system: System, configs: Configurations, **kwargs) -> None:
        super().__init__(system, configs, name=configs.get('General', 'id'), **kwargs)

    def _configure(self, configs: Configurations, **kwargs) -> None:
        super()._configure(configs, **kwargs)

        self.surface_tilt = configs.getfloat('Mounting', 'tilt', fallback=0)
        self.surface_azimuth = configs.getfloat('Mounting', 'azimuth', fallback=180)

        self.surface_type = configs.getint('General', 'ground_type', fallback=None)
        if self.surface_type is not None:
            from pvlib import irradiance
            self.albedo = irradiance.SURFACE_ALBEDOS.get(self.surface_type, 0.25)
        else:
            self.albedo = configs.getfloat('General', 'albedo', fallback=0.25)

        self.strings_per_inverter = configs.getint('Inverter', 'strings', fallback=1)
        self.modules_per_string = configs.getint('Module', 'count', fallback=1)

    @property
    def type(self) -> str:
        return 'pv'
