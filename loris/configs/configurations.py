# -*- coding: utf-8 -*-
"""
loris.configs.configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import os
import shutil
from collections import OrderedDict
from collections.abc import Mapping, MutableMapping
from copy import deepcopy
from typing import Any, List, Optional

import pandas as pd
from loris import LocalResourceException, LocalResourceUnavailableException
from loris.configs import Directories
from loris.util import to_bool, to_date, to_float, to_int


class Configurations(MutableMapping[str, Any]):
    @classmethod
    def load(
        cls,
        conf_file: str,
        conf_dir: str = None,
        cmpt_dir: str = None,
        data_dir: str = None,
        tmp_dir: str = None,
        log_dir: str = None,
        lib_dir: str = None,
        flat: bool = False,
        require: bool = True,
        **kwargs,
    ) -> Configurations:
        if not conf_dir and flat:
            conf_dir = ""

        conf_dirs = Directories(lib_dir, log_dir, tmp_dir, data_dir, cmpt_dir, conf_dir)
        conf_path = os.path.join(conf_dirs.conf, conf_file)

        if os.path.isdir(conf_dirs.conf):
            if not os.path.isfile(conf_path):
                config_default = conf_path.replace(".conf", ".default.conf")
                if os.path.isfile(config_default):
                    shutil.copy(config_default, conf_path)
        elif require:
            raise ConfigurationUnavailableException(f"Invalid configuration directory: {conf_dirs.conf}")

        configs = cls(conf_file, conf_path, conf_dirs, **kwargs)
        configs._load(require)
        return configs

    def _load(self, require: bool = True) -> None:
        if os.path.isfile(self.__path):
            # TODO: Implement other configuration parsers
            self._load_toml(self.__path)

        elif require:
            raise ConfigurationUnavailableException(f'Invalid configuration file "{self.__path}"')

        for section in self.sections:
            section._load(require=False)

    def _load_toml(self, config_path: str) -> None:
        from .toml import load_toml

        self.update(load_toml(config_path))

    def __init__(
        self,
        name: str,
        path: str,
        dirs: Directories,
        defaults: Optional[Mapping[str, Any]] = None,
        **kwargs
    ) -> None:
        super().__init__()
        self.__configs = OrderedDict()
        self.__name = name
        self.__path = path
        self.__dirs = dirs

        if defaults is not None:
            self.__update(defaults)
        self.__update(kwargs)
        self.__dirs.join(self)

    def __repr__(self) -> str:
        represent_header = self.name.replace(".conf", "")
        represent_configs = deepcopy(self.__configs)
        for represent_section in [s for s, c in self.__configs.items() if isinstance(c, Mapping)]:
            represent_configs.move_to_end(represent_section)

        def represent_sections(header: str, section: Mapping[str, Any]) -> str:
            representation = f"[{header}]\n"
            for k, v in section.items():
                if isinstance(v, Mapping):
                    representation += "\n" + represent_sections(f"{header}.{k}", v)
                else:
                    representation += f"{k} = {v}\n"
            return representation

        # representation_dirs = "\n" + str(self.dirs).replace(
        #     f"[{Directories.SECTION}]", f"[{represent_header}.{Directories.SECTION}]"
        # )
        return represent_sections(represent_header, represent_configs)  # + representation_dirs

    def __delitem__(self, key: str) -> None:
        del self.__configs[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.set(key, value)

    def set(self, key: str, value: Any) -> None:
        self.__configs[key] = value

    def __getitem__(self, key: str) -> Any:
        return self.__configs[key]

    def get(self, key: str, default: Any = None) -> Any:
        if default is None:
            return self.__configs.get(key)
        return self.__configs.get(key, default)

    def get_bool(self, key, default: bool = None) -> bool:
        return to_bool(self.get(key, default))

    def get_int(self, key, default: int = None) -> int:
        return to_int(self.get(key, default))

    def get_float(self, key, default: float = None) -> float:
        return to_float(self.get(key, default))

    def get_date(self, key, default: dt.datetime | pd.Timestamp = None) -> pd.Timestamp:
        return to_date(self.get(key, default))

    def __iter__(self):
        return iter(self.__configs)

    def __len__(self) -> int:
        return len(self.__configs)

    def move_to_top(self, key: str) -> None:
        self.__configs.move_to_end(key, False)

    def move_to_bottom(self, key: str) -> None:
        self.__configs.move_to_end(key, True)

    def copy(self):
        return Configurations(self.name, self.path, self.dirs, deepcopy(self.__configs))

    @property
    def name(self) -> str:
        return self.__name

    @property
    def path(self) -> str:
        return self.__path

    @property
    def dirs(self) -> Directories:
        return self.__dirs

    @property
    def enabled(self) -> bool:
        return to_bool(self.get("enabled", default=True)) and not to_bool(self.get("disabled", default=False))

    @property
    def sections(self) -> List[Configurations]:
        return [v for v in self.values() if isinstance(v, Configurations)]

    def has_section(self, section: str) -> bool:
        if section in [k for k, v in self.items() if isinstance(v, Configurations)]:
            return True
        return False

    def get_section(self, section: str, defaults: Optional[Mapping[str, Any]] = None) -> Configurations:
        if self.has_section(section):
            section = self[section]
        elif defaults is not None:
            section = self.__new_section(section, defaults)
        else:
            raise ConfigurationUnavailableException(f"Unknown configuration section: {section}")
        return section

    def __new_section(self, section, configs: Mapping[str, Any]) -> Configurations:
        if not isinstance(configs, Mapping):
            raise ConfigurationException(f"Invalid configuration '{section}': {type(configs)}")
        configs_name = f"{section}.conf"
        configs_path = os.path.join(self.__path.replace(".conf", ".d"), configs_name)
        return Configurations(configs_name, configs_path, self.__dirs, configs)

    def __update(self, u: Mapping[str, Any], replace: bool = True) -> Configurations:
        for k, v in u.items():
            if isinstance(v, Mapping):
                if k not in self.keys():
                    self[k] = self.__new_section(k, v)
                self[k] = Configurations.update(self[k], v)
            elif k not in self or replace:
                self[k] = v
        return self

    update = __update


class ConfigurationException(LocalResourceException):
    """
    Raise if a configuration is invalid.

    """


class ConfigurationUnavailableException(LocalResourceUnavailableException, ConfigurationException):
    """
    Raise if a configuration file can not be found.

    """
