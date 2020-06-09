# -*- coding: utf-8 -*-
"""
    th-e-core.configs
    ~~~~~~~~~~~~~~~~
    
    
"""
import logging
logger = logging.getLogger(__name__)

import os
from configparser import ConfigParser


class Configurable:

    def __init__(self, configs, *_, **kwargs):
        super().__init__(**kwargs)
        
        if not isinstance(configs, ConfigParser):
            raise ValueError('Invalid configuration type: {}'.format(type(configs)))
        
        self._configs = configs
        self._configure(configs, **kwargs)

#     def __repr__(self):
#         configs = '[{}]'.format(self._class_name)
#         for section in self._configs.sections():
#             configs += '\n    [{}]'.format(section) + '\n'
#             for (k, v) in self._configs.items(section):
#                 configs += '        {} = {}'.format(k, v) + '\n'
#         
#         return configs
#
#     def _configure(self, configs, **kwargs): #@UnusedVariable
#         if logger.isEnabledFor(logging.DEBUG):
#             print(self)

    def _configure(self, configs, **kwargs):
        pass

    @classmethod
    def _read(cls, 
             root_dir='.', 
             lib_dir='lib', 
             tmp_dir='tmp', 
             data_dir='data', 
             config_dir='conf', 
             config_name=None, 
             **kwargs):
        
        if config_name is None:
            config_name = cls.__name__.lower() + '.cfg'
        
        configs = cls._read_configs(root_dir, lib_dir, tmp_dir, data_dir, config_dir, config_name, **kwargs)
        
        package = kwargs.get('package') if 'package' in kwargs else '.'.join(cls.__module__.split('.')[:-1])
        module = kwargs.get('module') if 'module' in kwargs else cls.__module__.split('.')[-1]
        
        return cls._from_configs(configs, package, module, cls.__name__)

    @staticmethod
    def _read_configs(root_dir, lib_dir, tmp_dir, data_dir, config_dir, config_name, config_require=True, **_):
        if not os.path.isabs(config_dir):
            if os.path.isabs(data_dir):
                config_dir = os.path.join(data_dir, config_dir)
            else:
                config_dir = os.path.join(root_dir, config_dir)
                
        if not os.path.isdir(config_dir):
            raise ConfigUnavailableException('Invalid configuration directory: {}'.format(config_dir))
        
        configs = ConfigParser()
        configs.optionxform = str
        config_file = os.path.join(config_dir, config_name)
        if config_require:
            if not os.path.isfile(config_file):
                raise ConfigUnavailableException('Unable to find configuration file "{}"'.format(config_file))
            
            configs.read(config_file)
        
        if 'General' not in configs.sections():
            configs.add_section('General')
        
        if not configs.has_option('General', 'root_dir'):
            configs.set('General', 'root_dir', root_dir)
        if not configs.has_option('General', 'lib_dir'):
            configs.set('General', 'lib_dir', lib_dir if os.path.isabs(lib_dir) else os.path.join(root_dir, lib_dir))
        if not configs.has_option('General', 'tmp_dir'):
            configs.set('General', 'tmp_dir', tmp_dir if os.path.isabs(tmp_dir) else os.path.join(root_dir, tmp_dir))
        if not configs.has_option('General', 'data_dir'):
            configs.set('General', 'data_dir', data_dir if os.path.isabs(data_dir) else os.path.join(root_dir, data_dir))
        
        configs.set('General', 'config_dir', config_dir)
        configs.set('General', 'config_file', config_file)
        
        return configs

    @staticmethod
    def _from_configs(configs, pkg, mdl, cls, *args, **kwargs):
        if 'Import' not in configs.sections():
            configs.add_section('Import')
        
        if configs.has_option('General', 'type') and not configs.get('General', 'type').lower() == 'default':
            configs.set('Import', 'class', configs.get('General', 'type'))
        elif not configs.has_option('Import', 'class'):
            configs.set('Import', 'class', cls)
        if not configs.has_option('Import', 'module'):
            configs.set('Import', 'module', mdl)
        if not configs.has_option('Import', 'package'):
            configs.set('Import', 'package', pkg)
        
        try:
            obj = __import__(configs['Import']['package']+'.'+configs['Import']['module'], 
                             fromlist=[configs['Import']['class']])
            
        except ModuleNotFoundError as error:
            logger.debug(error)
            
            configs.set('Import', 'package', 'th_e_core')
            obj = __import__('th_e_core.'+configs['Import']['module'], fromlist=[configs['Import']['class']])
        
        return getattr(obj, configs['Import']['class'])(configs, *args, **kwargs)

    @property
    def _class_name(self):
        return type(self).__name__


class ConfigUnavailableException(Exception):
    """
    Raise if a configuration file can not be found.
    
    """

