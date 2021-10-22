# -*- coding: utf-8 -*-
"""
    th-e-core.configs
    ~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

import os
import logging
from configparser import ConfigParser as Configurations
from th_e_core.tools import _path

logger = logging.getLogger(__name__)


class Configurable:

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(**kwargs)
        self._configs = configs
        self._configure(configs, **kwargs)

#    def __repr__(self) -> str:
#        configs = '[{}]'.format(self._class_name)
#        for section in self._configs.sections():
#            if section == 'Import':
#                continue
#
#            configs += '\n    [{}]'.format(section) + '\n'
#
#            if section == 'General':
#                if self._configs.has_option(section, 'id'):
#                    configs_id = self._configs.get(section, 'id')
#                    configs += '        id = ' + configs_id + '\n'
#                if self._configs.has_option(section, 'name'):
#                    configs_name = self._configs.get(section, 'name')
#                    configs += '        name = ' + configs_name + '\n'
#
#            for (k, v) in self._configs.items(section):
#                if section == 'General' and \
#                        k in ['id', 'name', 'root_dir', 'lib_dir', 'tmp_dir', 'data_dir', 'config_dir', 'config_file']:
#                    continue
#
#                configs += '        {} = {}'.format(k, v) + '\n'
#
#        return configs

    @property
    def configs(self) -> Configurations:
        return self._configs

    def _configure(self, configs: Configurations, **kwargs) -> None:
        # if logger.isEnabledFor(logging.DEBUG):
        #     print(self)
        pass

    @classmethod
    def _read(cls, 
              root_dir:    str = '.',
              lib_dir:     str = 'lib',
              tmp_dir:     str = 'tmp',
              data_dir:    str = 'data',
              config_dir:  str = 'conf',
              config_name: str = None,
              **kwargs) -> Configurable:

        if config_name is None:
            config_name = cls.__name__.lower() + '.cfg'

        configs = cls._read_configs(root_dir, lib_dir, tmp_dir, data_dir, config_dir, config_name, **kwargs)

        package = kwargs.get('package') if 'package' in kwargs else '.'.join(cls.__module__.split('.')[:-1])
        module = kwargs.get('module') if 'module' in kwargs else cls.__module__.split('.')[-1]

        return cls._from_configs(configs, package, module, cls.__name__)

    @staticmethod
    def _read_configs(root_dir: str,
                      lib_dir: str,
                      tmp_dir: str,
                      data_dir: str,
                      config_dir: str,
                      config_name: str,
                      config_require: bool = True, **_) -> Configurations:

        if "~" in config_dir:
            config_dir = os.path.expanduser(config_dir)
        if not os.path.isabs(config_dir):
            if data_dir == 'data':
                config_dir = os.path.join(root_dir, config_dir)
            elif os.path.isabs(data_dir):
                config_dir = os.path.join(data_dir, config_dir)
            elif "~" in data_dir:
                config_dir = os.path.join(os.path.expanduser(data_dir), config_dir)
            else:
                config_dir = os.path.join(root_dir, data_dir, config_dir)

        if not os.path.isdir(config_dir):
            raise ConfigurationUnavailableException('Invalid configuration directory: {}'.format(config_dir))

        configs = Configurations()
        configs.optionxform = str
        config_file = os.path.join(config_dir, config_name)
        if config_require:
            if not os.path.isfile(config_file):
                raise ConfigurationUnavailableException('Unable to find configuration file "{}"'.format(config_file))
            
            configs.read(config_file)

        if 'General' not in configs.sections():
            configs.add_section('General')

        configs.set('General', 'root_dir', _path(configs, 'root_dir', root_dir))
        configs.set('General', 'lib_dir', _path(configs, 'lib_dir', lib_dir))
        configs.set('General', 'tmp_dir', _path(configs, 'tmp_dir', tmp_dir))
        configs.set('General', 'data_dir', _path(configs, 'data_dir', data_dir))
        configs.set('General', 'config_dir', config_dir)
        configs.set('General', 'config_file', config_file)

        return configs

    @staticmethod
    def _from_configs(configs: Configurations, pkg: str, mdl: str, cls: str, *args, **kwargs) -> Configurable:
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
    def _class_name(self) -> str:
        return type(self).__name__


class ConfigurationException(Exception):
    """
    Raise if a configuration is invalid.

    """
    pass


class ConfigurationUnavailableException(ConfigurationException):
    """
    Raise if a configuration file can not be found.
    
    """
    pass
