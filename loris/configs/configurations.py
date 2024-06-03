# -*- coding: utf-8 -*-
"""
    loris.core.configs.configurations
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Optional, Any
from collections import OrderedDict
from collections.abc import MutableMapping, Mapping

import os
import shutil
import datetime as dt
import pandas as pd

from copy import deepcopy
from loris import LocalResourceException, LocalResourceUnavailableException
from loris.util import to_bool, to_int, to_float, to_date
from loris.configs import Directories


class Configurations(MutableMapping[str, Any]):

    @classmethod
    def load(cls,
             conf_file: str,
             conf_dir:  str = None,
             cmpt_dir: str = None,
             data_dir: str = None,
             tmp_dir: str = None,
             log_dir: str = None,
             lib_dir: str = None,
             flat: bool = False,
             require: bool = True,
             **kwargs) -> Configurations:

        if not conf_dir and flat:
            conf_dir = ''

        conf_dirs = Directories(lib_dir, log_dir, tmp_dir, data_dir, cmpt_dir, conf_dir)
        conf_path = os.path.join(conf_dirs.conf, conf_file)

        if os.path.isdir(conf_dirs.conf):
            if not os.path.isfile(conf_path):
                config_default = conf_path.replace('.conf', '.default.conf')
                if os.path.isfile(config_default):
                    shutil.copy(config_default, conf_path)
                elif require:
                    raise ConfigurationUnavailableException(f'Invalid configuration file "{conf_path}"')
        elif require:
            raise ConfigurationUnavailableException(f'Invalid configuration directory: {conf_dirs.conf}')

        configs = cls._load(conf_path)
        return cls(conf_file, conf_path, conf_dirs, configs, **kwargs)

    @staticmethod
    def _load(conf_path: str):
        # TODO: Implement other configuration parsers
        from .toml import load_toml
        return load_toml(conf_path)

    def __init__(self,
                 name: str,
                 path: str,
                 dirs: Directories,
                 other: Optional[Mapping[str, Any]] = None, **kwargs) -> None:
        super().__init__()
        self.name = name
        self.path = path

        if other is None:
            other = {}
        self._configs = OrderedDict(other, **kwargs)
        self._dirs = dirs
        self._dirs.join(self)

    def __repr__(self) -> str:
        represent_configs = deepcopy(self._configs)
        for represent_section in [s for s, c in self._configs.items() if isinstance(c, Mapping)]:
            represent_configs.move_to_end(represent_section)

        def represent_sections(header: str, section: Mapping[str, Any], ) -> str:
            representation = f'[{header}]\n'
            for (k, v) in section.items():
                if isinstance(v, Mapping):
                    representation += '\n' + represent_sections(f'{header}.{k}', v)
                else:
                    representation += f'{k} = {v}\n'
            return representation

        represent_prefix = self.name.replace('.conf', '')
        representation_dirs = '\n' + str(self.dirs).replace(f"[{Directories.SECTION}]",
                                                            f"[{represent_prefix}.{Directories.SECTION}]")
        return represent_sections(represent_prefix, represent_configs) + representation_dirs

    def __delitem__(self, key: str) -> None:
        del self._configs[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.set(key, value)

    def set(self, key: str, value: Any) -> None:
        self._configs[key] = value

    def __getitem__(self, key: str) -> Any:
        return self._configs[key]

    def get(self, key: str, default: Any = None) -> Any:
        if default is None:
            return self._configs.get(key)
        return self._configs.get(key, default)

    def get_bool(self, key, default: bool = None) -> bool:
        return to_bool(self.get(key, default))

    def get_int(self, key, default: int = None) -> int:
        return to_int(self.get(key, default))

    def get_float(self, key, default: float = None) -> float:
        return to_float(self.get(key, default))

    def get_date(self, key, default: dt.datetime | pd.Timestamp = None) -> pd.Timestamp:
        return to_date(self.get(key, default))

    def has_section(self, section: str) -> bool:
        try:
            self._get_section(self._configs, *section.split('.'))

        except ConfigurationUnavailableException:
            return False
        return True

    def get_section(self, section: str, default: Optional[Mapping[str, Any]] = None) -> Configurations:
        configs_name = f"{section.split('.')[-1]}.conf"
        configs_path = os.path.join(self.path.replace('.conf', '.d'), configs_name)
        configs = Configurations(configs_name, configs_path, self.dirs, default)
        try:
            configs.update(self._get_section(self._configs, *section.split('.')))

        except ConfigurationUnavailableException as e:
            if default is None:
                raise e
        return configs

    @staticmethod
    def _get_section(configs: Mapping[str, Any], *sections: str) -> Mapping[str, Any]:
        if len(sections) < 1:
            raise ValueError(f"'Invalid configuration section: {'.'.join(sections)}")
        sections = list(sections)
        section = sections.pop(0)
        if section not in configs.keys():
            raise ConfigurationUnavailableException(f"Unknown configuration section: {section}")
        if not isinstance(configs[section], Mapping):
            raise ConfigurationUnavailableException(f"Invalid configuration \"{section}\": {type(configs[section])}")

        if len(sections) >= 1:
            return Configurations._get_section(configs[section], *sections)
        return configs[section]

    def __iter__(self):
        return iter(self._configs)

    def __len__(self) -> int:
        return len(self._configs)

    def move_to_top(self, key: str) -> None:
        self._configs.move_to_end(key, False)

    def move_to_bottom(self, key: str) -> None:
        self._configs.move_to_end(key, True)

    def update(self, u: Mapping[str, Any], replace: bool = True, **kwargs: Any) -> Mapping[str, Any]:
        for k, v in u.items():
            if isinstance(v, Mapping):
                self[k] = Configurations.update(self.get(k, {}), v)
            elif k not in self or replace:
                self[k] = v
        return self

    def copy(self):
        return Configurations(self.name, self.path, self.dirs, deepcopy(self._configs))

    @property
    def dirs(self) -> Directories:
        return self._dirs

    @property
    def enabled(self) -> bool:
        return to_bool(self.get('enabled', default=True)) and not \
            to_bool(self.get('disabled', default=False))


class ConfigurationException(LocalResourceException):
    """
    Raise if a configuration is invalid.

    """
    pass


class ConfigurationUnavailableException(LocalResourceUnavailableException, ConfigurationException):
    """
    Raise if a configuration file can not be found.

    """
    pass
