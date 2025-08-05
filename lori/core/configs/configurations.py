# -*- coding: utf-8 -*-
"""
lori.core.configurations
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from collections import OrderedDict
from copy import deepcopy
from pathlib import Path
from typing import Any, Collection, Iterable, Iterator, List, Mapping, MutableMapping, Optional

import pandas as pd
from lori.core import ResourceException, ResourceUnavailableException
from lori.core.configs import Directories, Directory
from lori.typing import TimestampType
from lori.util import is_bool, to_bool, to_date, to_float, to_int, update_recursive


class Configurations(MutableMapping[str, Any]):
    @classmethod
    def load(
        cls,
        conf_file: str,
        conf_dir: str = None,
        data_dir: str = None,
        tmp_dir: str = None,
        log_dir: str = None,
        lib_dir: str = None,
        flat: bool = False,
        require: bool = True,
        **defaults,
    ) -> Configurations:
        if not conf_dir and flat:
            conf_dir = ""

        conf_dirs = Directories(lib_dir, log_dir, tmp_dir, data_dir, conf_dir)
        conf_path = Path(conf_dirs.conf, conf_file)

        if conf_dirs.conf.is_dir():
            if not conf_path.is_file():
                config_default = str(conf_path).replace(".conf", ".default.conf")
                if os.path.isfile(config_default):
                    shutil.copy(config_default, conf_path)
        elif require:
            raise ConfigurationUnavailableException(f"Invalid configuration directory: {conf_dirs.conf}")

        configs = cls(conf_file, conf_dirs, defaults)
        configs._load(require)
        return configs

    # noinspection PyProtectedMember
    def _load(self, require: bool = True) -> None:
        if self.__path.exists() and self.__path.is_file():
            try:
                # TODO: Implement other configuration parsers
                self._load_toml(str(self.__path))
            except Exception as e:
                raise ConfigurationUnavailableException(f"Error loading configuration file '{self.__path}': {str(e)}")

        elif require:
            raise ConfigurationUnavailableException(f"Invalid configuration file '{self.__path}'")

        if Directories.SECTION in self.__configs:
            self.__dirs.update(self.__configs[Directories.SECTION])

    def _load_toml(self, config_path: str) -> None:
        from .toml import load_toml

        self.update(load_toml(config_path))

    def __init__(
        self,
        name: str,
        dirs: Directories,
        defaults: Optional[Mapping[str, Any]] = None,
        **kwargs,
    ) -> None:
        super().__init__()
        self.__configs = OrderedDict()
        self.__dirs = dirs
        self.__path = Path(dirs.conf, name)

        if defaults is not None:
            self.update(defaults)
        self.update(kwargs)

    def __repr__(self) -> str:
        return f"{Configurations.__name__}({self.__path})"

    def __str__(self) -> str:
        # noinspection PyShadowingNames
        def parse_configs(header: str, configs: Configurations) -> str:
            configs = OrderedDict(configs)
            for section in [s for s, c in configs.items() if isinstance(c, Mapping)]:
                configs.move_to_end(section)

            string = f"[{header}]\n"
            for k, v in configs.items():
                if isinstance(v, Configurations):
                    string += "\n" + parse_configs(f"{header}.{k}", v)
                else:
                    string += f"{k} = {v}\n"
            return string

        return parse_configs(self.name.replace(".conf", ""), self)

    def __delitem__(self, key: str) -> None:
        del self.__configs[key]

    def remove(self, *keys: str) -> None:
        for key in keys:
            del self.__configs[key]

    def pop(self, key: str, default: Any = None) -> Any:
        value = self._get(key, default)
        if key in self.__configs:
            self.remove(key)
        return value

    def __setitem__(self, key: str, value: Any) -> None:
        self.set(key, value)

    # noinspection PyTypeChecker
    def set(self, key: str, value: Any, replace: bool = True) -> None:
        if isinstance(value, Mapping):
            if key not in self.__configs.keys():
                value = self._create_section(key, value)
            else:
                section = self.__configs[key]
                if isinstance(section, Mapping) and not replace:
                    value = update_recursive(section, value, replace=False)
            self.__configs[key] = value
        elif key not in self.__configs.keys() or replace:
            self.__configs[key] = value

    def __getitem__(self, key: str) -> Any:
        return self.__configs[key]

    def _get(self, key: str, default: Any = None) -> Any:
        return self.__configs.get(key, default)

    def get(self, key: str | Iterable[str], default: Any = None) -> Any:
        if not isinstance(key, Iterable) or isinstance(key, str):
            return self._get(key, default)
        return {
            k: self._get(k, default=default[k] if default is not None and isinstance(default, Mapping) else None)
            for k in key
            if k in self
        }

    def get_bool(self, key: str, default: bool = None) -> bool:
        return to_bool(self._get(key, default))

    def get_int(self, key: str, default: int = None) -> int:
        return to_int(self._get(key, default))

    def get_float(self, key: str, default: float = None) -> float:
        return to_float(self._get(key, default))

    def get_date(self, key: str, default: TimestampType = None, **kwargs) -> pd.Timestamp:
        return to_date(self._get(key, default), **kwargs)

    def __contains__(self, key: str) -> bool:
        return key in self.__configs

    def __iter__(self) -> Iterator[str]:
        return iter(self.__configs)

    def __len__(self) -> int:
        return len(self.__configs)

    def move_to_top(self, key: str) -> None:
        self.__configs.move_to_end(key, False)

    def move_to_bottom(self, key: str) -> None:
        self.__configs.move_to_end(key, True)

    def write(self) -> None:
        configs = {k: v for k, v in self.__configs.items() if k not in self.sections}

        if not self.__dirs.conf.exists():
            self.__dirs.conf.mkdir(parents=True, exist_ok=True)

        file_desc, file_path = tempfile.mkstemp(prefix=self.name, dir=self.dirs.conf)
        with os.fdopen(file_desc, "w") as file:
            lines = self.__read_lines()
            lines_section = len(lines) - 1
            for line_index, line in enumerate(lines):
                if "=" in line:
                    line = line.rstrip()
                    key, value, *_ = line.split("=")
                    key = key.lstrip().lstrip("#").lstrip(";").strip()
                    value = value.strip().strip('"')
                    if key in configs:
                        config_value = str(configs.pop(key))
                        if config_value.lower() != value.lower() or line.lstrip().startswith(("#", ";")):
                            lines[line_index] = self.__parse_line(key, config_value)

                if re.match(r"(#.*|;.*|)\[.*?]", line):
                    lines_section = line_index - 1
                    break
            while lines_section > 0 and lines[lines_section - 1].strip() == "":
                lines_section -= 1

            if len(configs) > 0:
                if len(lines) > 0:
                    lines.insert(lines_section, "\n")
                    lines_section += 1
                for key, value in configs.items():
                    lines.insert(lines_section, self.__parse_line(key, value))
                    lines_section += 1

            file.writelines(lines)

        # Copy the file permissions from the configuration file to the temporary file and remove it
        if self.__path.exists():
            shutil.copymode(self.__path, file_path)
            os.remove(self.__path)

        shutil.move(file_path, self.__path)

    def __read_lines(self) -> List[str]:
        if not self.__path.exists():
            return []
        with open(self.__path, "r") as file:
            return file.readlines()

    # noinspection PyMethodMayBeStatic
    def __parse_line(self, key, value: Any) -> str:
        if is_bool(value):
            value = str(value).lower()
        elif isinstance(value, str):
            if "\\" in value:
                value = value.translate(str.maketrans({"\\": r"\\"}))
            value = f'"{value}"'
        return f"{key} = {value}\n"

    def copy(self, dirs: Optional[Directories] = None) -> Configurations:
        if dirs is None:
            dirs = deepcopy(self.dirs)
        elif dirs.conf != self.dirs.conf:
            self.__copy_path(self.__path.parents[0], dirs.conf, self.name)
            self.__copy_path(self.__path.parents[0], dirs.conf, self.name.replace(".conf", ".d"))
            for section in self.sections:
                section_dir = dirs.conf.joinpath(self.name.replace(".conf", ".d"))
                self.__copy_path(self.__path.parents[0], section_dir, f"{section}.conf")

        return Configurations(self.name, dirs, deepcopy(self.__configs))

    @staticmethod
    def __copy_path(source: Path, destination: Path, name: str) -> None:
        source = source.joinpath(name)
        destination = destination.joinpath(name)
        if not source.exists():
            return

        destination.parents[0].mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, destination, ignore=_include(r".*\.conf"), dirs_exist_ok=True)
        elif not destination.exists():
            shutil.copy2(source, destination)

    @property
    def key(self) -> str:
        return str(self.__path.name.removesuffix(".conf"))

    @property
    def name(self) -> str:
        return str(self.__path.name)

    @property
    def path(self) -> str:
        return str(self.__path)

    @property
    def dirs(self) -> Directories:
        return self.__dirs

    @property
    def enabled(self) -> bool:
        return to_bool(self._get("enabled", default=True)) and not to_bool(self._get("disabled", default=False))

    @enabled.setter
    def enabled(self, enabled: bool) -> None:
        self.set("enabled", enabled)

    @property
    def sections(self) -> List[str]:
        return [k for k, v in self.items() if isinstance(v, Configurations)]

    @property
    def _sections_dir(self) -> Directory:
        return self.__dirs.conf.joinpath(self.__path.name.replace(".conf", ".d"))

    def has_section(self, section: str, includes: bool = False) -> bool:
        if section in self.sections:
            return True
        if includes and self._sections_dir.joinpath(f"{section}.conf").exists():
            return True
        return False

    def get_sections(
        self,
        sections: Collection[str],
        ensure_exists: bool = False,
    ) -> Configurations:
        sections = {
            s: self.get_section(s, defaults={}, ensure_exists=ensure_exists)
            for s in sections
            if s in self.sections or ensure_exists
        }
        section_dirs = self.__dirs.copy()
        section_dirs.conf = self._sections_dir
        return Configurations(self.name, section_dirs, sections)

    def get_section(
        self,
        section: str,
        defaults: Optional[Mapping[str, Any]] = None,
        ensure_exists: bool = False,
    ) -> Configurations:
        if not self.has_section(section) and ensure_exists:
            if defaults is None:
                defaults = {}
            self._add_section(section, defaults)
            return self[section]

        elif self.has_section(section):
            configs = self[section]
            if defaults is not None:
                configs.update(defaults, replace=False)
            return configs

        elif defaults is not None:
            return self._create_section(section, defaults)
        else:
            raise ConfigurationUnavailableException(f"Unknown configuration section: {section}")

    def _add_section(self, section, configs: Mapping[str, Any]) -> None:
        if self.has_section(section):
            raise ConfigurationUnavailableException(f"Unable to add existing configuration section: {section}")
        self[section] = self._create_section(section, configs)

    def _create_section(self, section, configs: Mapping[str, Any]) -> Configurations:
        if not isinstance(configs, Mapping):
            raise ConfigurationException(f"Invalid configuration '{section}': {type(configs)}")
        section_name = f"{section}.conf"
        section_dirs = self.__dirs.copy()
        section_dirs.conf = self._sections_dir
        section_configs = Configurations(section_name, section_dirs, configs)
        section_configs._load(require=False)
        return section_configs

    def pop_section(
        self,
        section: str,
        defaults: Optional[Mapping[str, Any]] = None,
    ) -> Configurations:
        section_configs = self.get_section(section, defaults=defaults)
        if section in self.__configs:
            self.remove(section)
        return section_configs

    # noinspection PyTypeChecker
    def update(self, update: Mapping[str, Any], replace: bool = True) -> None:
        update_recursive(self, update, replace=replace)


class ConfigurationException(ResourceException):
    """
    Raise if a configuration is invalid.

    """


class ConfigurationUnavailableException(ResourceUnavailableException, ConfigurationException):
    """
    Raise if a configuration file can not be found.

    """


def _include(pattern):
    def _ignore(path, names):
        return set(n for n in names if not re.match(pattern, n) and not os.path.isdir(os.path.join(path, n)))

    return _ignore
