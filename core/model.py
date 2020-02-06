# -*- coding: utf-8 -*-
"""
    th-e-core.model
    ~~~~~~~~~~~~~~~
    
    
"""
import logging
logger = logging.getLogger(__name__)

import os
from configparser import ConfigParser
from abc import ABC, abstractmethod


class Model(ABC):

    def __init__(self, context, configs, **kwargs):
        from core import System
        if not isinstance(context, System):
            raise TypeError('Invalid model system type: {}'.format(type(context)))
        if not isinstance(configs, ConfigParser):
            raise ValueError('Invalid model configuration type: {}'.format(type(configs)))
        
        self._context = context
        self._configs = configs
        self._configure(configs, **kwargs)
        self._build(context, **kwargs)

    @staticmethod
    def build(system, **kwargs):
        from core import System
        if not isinstance(system, System):
            raise TypeError('Invalid model system type: {}'.format(type(system)))
        
        config_dir = system._configs.get('General', 'config_dir')
        if not os.path.isdir(config_dir):
            raise ValueError('Invalid configuration directory: {}'.format(config_dir))
        
        config_file = os.path.join(config_dir, 'model.cfg')
        if not os.path.isfile(config_file):
            raise ValueError('Unable to open model configurations: {}'.format(config_file))
        
        configs = ConfigParser()
        configs.read(config_file)
        
        if 'General' not in configs.sections():
            raise ValueError('Incomplete model configuration. Unable to find general section')
        
        if 'type' not in configs['General']:
            raise ValueError('Incomplete model configuration. Unable to find model type')
        if 'module' not in configs['General']:
            configs.set('General', 'module', 'model')
        if 'package' not in configs['General']:
            package = kwargs.get('package') if 'package' in kwargs else system._configs.get('General', 'package')
            configs.set('General', 'package', package)
        
        module = __import__(configs['General']['package']+'.'+configs['General']['module'], 
                            fromlist=[configs['General']['type']])
        model = getattr(module, configs['General']['type'])(system, configs, **kwargs)
        
        if not isinstance(model, Model):
            raise TypeError('Invalid model type: {}'.format(type(model)))
        
        return model

    def _build(self, system, **_):
        pass

    def _configure(self, configs, **_):
        if logger.isEnabledFor(logging.DEBUG):
            text = ''
            for section in configs.sections():
                text += '\n        [{}]'.format(section) + '\n'
                for (k, v) in configs.items(section):
                    text += '            {} = {}'.format(k, v) + '\n'
            
            print('    [Model]{}'.format(text))

    @abstractmethod
    def run(self, **kwargs):
        pass

