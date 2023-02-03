# -*- coding: utf-8 -*-
"""
    th-e-core.configs
    ~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Dict

import os
import shutil
import logging
from configparser import ConfigParser

logger = logging.getLogger(__name__)


class Configurations(ConfigParser):

    GENERAL = 'General'

    @classmethod
    def from_configs(cls,
                     configs: Configurations,
                     conf_file: str = None,
                     conf_dir: str = None) -> Configurations:
        if conf_file is None:
            conf_file = cls.__name__.lower() + '.cfg'
        conf_dirs = configs.dirs.encode()
        if conf_dir is not None:
            conf_dirs['conf_dir'] = conf_dir
        return cls(conf_file, **conf_dirs)

    def __init__(self,
                 conf_file: str,
                 conf_dir:  str = None,
                 cmpt_dir: str = None,
                 data_dir: str = None,
                 tmp_dir: str = None,
                 log_dir: str = None,
                 lib_dir: str = None,
                 require: bool = True,
                 **kwargs) -> None:
        super().__init__()
        self.optionxform = str

        dirs = Directories(lib_dir, log_dir, tmp_dir, data_dir, cmpt_dir, conf_dir)

        conf_path = os.path.join(dirs.conf, conf_file)
        if os.path.isdir(dirs.conf):
            if not os.path.isfile(conf_path):
                config_default = conf_path.replace('.cfg', '.default.cfg')
                if os.path.isfile(config_default):
                    shutil.copy(config_default, conf_path)
                elif require:
                    raise ConfigurationUnavailableException('Invalid configuration file "{}"'.format(conf_path))
        elif require:
            raise ConfigurationUnavailableException('Invalid configuration directory: {}'.format(dirs.conf))

        if os.path.isfile(conf_path):
            self.read(conf_path, encoding='utf-8')

        if len(kwargs) > 0:
            if self.GENERAL not in self.sections():
                self.add_section(self.GENERAL)
            for kw, arg in kwargs.items():
                self.set(self.GENERAL, kw, arg)

        self.dirs = dirs.join(self)
        self.path = conf_path
        self.name = conf_file

    @property
    def dirs(self) -> Directories:
        return self._dirs

    @dirs.setter
    def dirs(self, dirs: Directories) -> None:
        self._dirs = dirs

    @property
    def general(self) -> Dict[str, str]:
        return dict(self.items(self.GENERAL))


class Configurable:

    @classmethod
    def _read(cls, conf_file: str = None, **kwargs) -> Configurable:
        if conf_file is None:
            conf_file = cls.__name__.lower() + '.cfg'

        return cls(Configurations(conf_file), **kwargs)

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._configs = configs
        self.__configure__(configs)

    def __configure__(self, configs: Configurations) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            print(self)

    def __repr__(self) -> str:
        configs = f"{self._class_name}:\n"
        for section in self.configs.sections():
            if section == 'General':
                if self.configs.has_option(section, 'id'):
                    configs_id = self.configs.get(section, 'id')
                    configs += f"\tid = {configs_id}\n"
                if self.configs.has_option(section, 'name'):
                    configs_name = self.configs.get(section, 'name')
                    configs += f"\tname = {configs_name}\n"

                for (k, v) in self.configs.items(section):
                    if k in ['id', 'name']:
                        continue
                    configs += f"\t{k} = {v}\n"
            else:
                configs += f"\n\t{section}:\n"
                for (k, v) in self.configs.items(section):
                    configs += f"\t\t{k} = {v}\n"

        return configs

    @property
    def configs(self) -> Configurations:
        return self._configs

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


class Directories:

    def __init__(self,
                 lib_dir: str = None,
                 log_dir: str = None,
                 tmp_dir: str = None,
                 data_dir: str = None,
                 cmpt_dir: str = None,
                 conf_dir:  str = None):

        self._run = os.getcwd()
        self._lib = lib_dir
        self._log = log_dir
        self._tmp = tmp_dir
        self._data = data_dir
        self._conf = conf_dir
        self._cmpt = cmpt_dir

    def __repr__(self):
        attrs = ['conf', 'cmpt', 'data', 'tmp', 'log', 'lib']
        return ('Directories:\n\t' + '\n\t'.join(
            f'{attr}: {str(getattr(self, attr))}' for attr in attrs))

    def encode(self) -> Dict[str, str]:
        dirs = {
            'lib_dir': self._lib,
            'log_dir': self._log,
            'tmp_dir': self._tmp,
            'data_dir': self._data
        }
        if self._cmpt is None or not os.path.isabs(self._cmpt):
            dirs['cmpt_dir'] = self._cmpt
        if self._conf is None or not os.path.isabs(self._conf):
            dirs['conf_dir'] = self._conf
        return dirs

    @property
    def lib(self):
        lib_dir = self._expand(self._lib) if self._lib is not None else 'lib'
        if not os.path.isabs(lib_dir):
            lib_dir = os.path.join(self._run, lib_dir)
        return lib_dir

    @property
    def log(self):
        log_dir = self._expand(self._log) if self._log is not None else 'log'
        if not os.path.isabs(log_dir):
            log_dir = os.path.join(self._run, log_dir)
        return log_dir

    @property
    def tmp(self):
        tmp_dir = self._expand(self._tmp) if self._tmp is not None else 'tmp'
        if not os.path.isabs(tmp_dir):
            tmp_dir = os.path.join(self._run, tmp_dir)
        return tmp_dir

    @property
    def data(self):
        data_dir = self._expand(self._data) if self._data is not None else 'data'
        if not os.path.isabs(data_dir):
            data_dir = os.path.join(os.getcwd(), data_dir)
        return data_dir

    @property
    def conf(self):
        conf_dir = self._expand(self._conf) if self._conf is not None else 'conf'
        if not os.path.isabs(conf_dir):
            if self._data is None or self._run == os.path.dirname(self.data):
                conf_dir = os.path.join(self._run, conf_dir)
            else:
                conf_dir = os.path.join(self.data, conf_dir)
        return conf_dir

    @property
    def cmpt(self):
        cmpt_dir = self._expand(self._cmpt) if self._cmpt is not None else self.conf
        if not os.path.isabs(cmpt_dir):
            cmpt_dir = os.path.join(os.getcwd(), cmpt_dir)
        return cmpt_dir

    def join(self, configs: Configurations) -> Directories:
        def join_config(key: str) -> None:
            if configs.has_option(Configurations.GENERAL, f'{key}_dir'):
                setattr(self, f'_{key}', configs.get(Configurations.GENERAL, f'{key}_dir'))

        join_config('lib')
        join_config('log')
        join_config('tmp')
        join_config('data')

        return self

    # noinspection PyShadowingBuiltins
    @staticmethod
    def _expand(dir: str) -> str:
        if "~" in dir:
            return os.path.expanduser(dir)
        return dir
