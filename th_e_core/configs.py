# -*- coding: utf-8 -*-
"""
    th-e-core.configs
    ~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations

import os
import shutil
import logging
from shutil import copytree, ignore_patterns
from configparser import ConfigParser as Configurations
from th_e_core.tools import join_path

logger = logging.getLogger(__name__)


def read(config_file: str,
         config_dir:  str = 'conf',
         data_dir: str = 'data',
         tmp_dir: str = 'tmp',
         lib_dir: str = 'lib',
         root_dir: str = '.', **_) -> Configurations:

    if "~" in data_dir:
        data_dir = os.path.expanduser(data_dir)
    if not os.path.isabs(data_dir):
        data_dir = os.path.join(root_dir, data_dir)

    if "~" in config_dir:
        config_dir = os.path.expanduser(config_dir)
    if not os.path.isabs(config_dir):
        if data_dir == os.path.join(root_dir, 'data'):
            config_dir = os.path.join(root_dir, config_dir)
        else:
            config_dir = os.path.join(data_dir, config_dir)

    if not os.path.isdir(config_dir):
        raise ConfigurationUnavailableException('Invalid configuration directory: {}'.format(config_dir))

    configs = Configurations()
    configs.optionxform = str
    config_file = os.path.join(config_dir, config_file)
    if not os.path.isfile(config_file):
        config_default = config_file.replace('.cfg', '.default.cfg')
        if os.path.isfile(config_default):
            shutil.copy(config_default, config_file)
        else:
            raise ConfigurationUnavailableException('Unable to find configuration file "{}"'.format(config_file))

    configs.read(config_file)

    if 'General' not in configs.sections():
        configs.add_section('General')

    configs.set('General', 'root_dir', join_path(configs, 'root_dir', root_dir))
    configs.set('General', 'lib_dir', join_path(configs, 'lib_dir', lib_dir))
    configs.set('General', 'tmp_dir', join_path(configs, 'tmp_dir', tmp_dir))
    configs.set('General', 'data_dir', join_path(configs, 'data_dir', data_dir))
    configs.set('General', 'config_dir', config_dir)
    # configs.set('General', 'config_file', config_file)

    return configs


class Configurable:

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(**kwargs)
        self._configs = configs
        self._configure(configs)

    # def __repr__(self) -> str:
    #     configs = '[{}]'.format(self._class_name)
    #     for section in self._configs.sections():
    #         if section == 'Import':
    #             continue
    #
    #         configs += '\n    [{}]'.format(section) + '\n'
    #
    #         if section == 'General':
    #             if self._configs.has_option(section, 'id'):
    #                 configs_id = self._configs.get(section, 'id')
    #                 configs += '        id = ' + configs_id + '\n'
    #             if self._configs.has_option(section, 'name'):
    #                 configs_name = self._configs.get(section, 'name')
    #                 configs += '        name = ' + configs_name + '\n'
    #
    #         for (k, v) in self._configs.items(section):
    #             if section == 'General' and \
    #                     k in ['id', 'name', 'root_dir', 'lib_dir', 'tmp_dir', 'data_dir', 'config_dir', 'config_file']:
    #                 continue
    #
    #             configs += '        {} = {}'.format(k, v) + '\n'
    #
    #     return configs

    @property
    def configs(self) -> Configurations:
        return self._configs

    def _configure(self, configs: Configurations) -> None:
        # if logger.isEnabledFor(logging.DEBUG):
        #    print(self)
        pass

    @classmethod
    def _read(cls, 
              root_dir:    str = '.',
              lib_dir:     str = 'lib',
              tmp_dir:     str = 'tmp',
              data_dir:    str = 'data',
              config_dir:  str = 'conf',
              config_file: str = None,
              **kwargs) -> Configurable:

        if config_file is None:
            config_file = cls.__name__.lower() + '.cfg'

        configs = cls._read_configs(root_dir, lib_dir, tmp_dir, data_dir, config_dir, config_file, **kwargs)
        return cls._from_class(configs)

    @staticmethod
    def _read_configs(root_dir: str,
                      lib_dir: str,
                      tmp_dir: str,
                      data_dir: str,
                      config_dir: str,
                      config_file: str,
                      config_scan: bool = False,
                      config_copy: bool = False,
                      config_require: bool = True, **_) -> Configurations:

        if "~" in data_dir:
            data_dir = os.path.expanduser(data_dir)
        if not os.path.isabs(data_dir):
            if data_dir == 'data':
                config_copy = False
            data_dir = os.path.join(root_dir, data_dir)

        if "~" in config_dir:
            config_dir = os.path.expanduser(config_dir)
        if config_dir == os.path.join(root_dir, 'conf') and data_dir != os.path.join(root_dir, 'data'):
            config_dir = os.path.join(data_dir, 'conf')
        elif not os.path.isabs(config_dir):
            if data_dir == os.path.join(root_dir, 'data'):
                config_dir = os.path.join(root_dir, config_dir)
            else:
                config_dir = os.path.join(data_dir, config_dir)

        if not os.path.isdir(config_dir):
            if config_copy and not config_scan:
                if not os.path.exists(data_dir):
                    os.makedirs(data_dir)
                config_defaults = os.path.join(root_dir, 'conf')
                copytree(config_defaults, config_dir, ignore=ignore_patterns('*.default.cfg', 'settings*', 'logging*'))
            else:
                raise ConfigurationUnavailableException('Invalid configuration directory: {}'.format(config_dir))

        configs = Configurations()
        configs.optionxform = str
        config_file = os.path.join(config_dir, config_file)
        if config_require:
            if not os.path.isfile(config_file):
                raise ConfigurationUnavailableException('Unable to find configuration file "{}"'.format(config_file))
            
            configs.read(config_file)

        if 'General' not in configs.sections():
            configs.add_section('General')

        configs.set('General', 'root_dir', join_path(configs, 'root_dir', root_dir))
        configs.set('General', 'lib_dir', join_path(configs, 'lib_dir', lib_dir))
        configs.set('General', 'tmp_dir', join_path(configs, 'tmp_dir', tmp_dir))
        configs.set('General', 'data_dir', join_path(configs, 'data_dir', data_dir))
        configs.set('General', 'config_dir', config_dir)
        configs.set('General', 'config_file', config_file)

        return configs

    # noinspection PyShadowingBuiltins
    @staticmethod
    def _from_configs(configs: Configurations,
                      package: str = None,
                      module: str = None,
                      type: str = None,
                      *args, **kwargs) -> Configurable:
        if 'Import' not in configs.sections():
            configs.add_section('Import')

        if configs.has_option('General', 'type') and not configs.get('General', 'type').lower() == 'default':
            configs.set('Import', 'class', configs.get('General', 'type'))
        elif not configs.has_option('Import', 'class'):
            configs.set('Import', 'class', type)
        if not configs.has_option('Import', 'module'):
            configs.set('Import', 'module', module)
        if not configs.has_option('Import', 'package'):
            configs.set('Import', 'package', package)

        try:
            obj = __import__(configs['Import']['package']+'.'+configs['Import']['module'], 
                             fromlist=[configs['Import']['class']])

        except ModuleNotFoundError as error:
            logger.debug(error)

            configs.set('Import', 'package', 'th_e_core')
            obj = __import__('th_e_core.'+configs['Import']['module'], fromlist=[configs['Import']['class']])

        return getattr(obj, configs['Import']['class'])(configs, *args, **kwargs)

    # noinspection PyShadowingBuiltins
    @classmethod
    def _from_class(cls, configs: Configurations, *args, **kwargs) -> Configurable:
        package = kwargs.get('package') if 'package' in kwargs else '.'.join(cls.__module__.split('.')[:-1])
        module = kwargs.get('module') if 'module' in kwargs else cls.__module__.split('.')[-1]
        type = cls.__name__

        return cls._from_configs(configs, package, module, type)

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
