# -*- coding: utf-8 -*-
"""
    th-e-core.model
    ~~~~~~~~~~~~~~~
    
    
"""
import os

from configparser import ConfigParser
from abc import ABC, abstractmethod

CONFIGS_DEFAULT = os.path.join('conf', 'model.cfg')


class Model(ABC):

    def __init__(self, configdir=CONFIGS_DEFAULT, **kwargs):
        if not os.path.isdir(configdir):
            raise ValueError('Invalid configuration directory: {}'.format(configdir))
        
        configfile = os.path.join(configdir, 'model.cfg')
        if not os.path.isfile(configfile):
            raise ValueError('Unable to open configurations: {}'.format(configfile))
        
        configs = ConfigParser()
        configs.read(configfile)
        self._configure(configs, **kwargs)

    def _configure(self, configs, **kwargs):
        pass

    @abstractmethod
    def run(self, start, end=None, **kwargs):
        pass

