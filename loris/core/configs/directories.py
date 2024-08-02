# -*- coding: utf-8 -*-
"""
loris.core.configs.directories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path, PosixPath, WindowsPath
from typing import Dict, Optional


class Directories:
    SECTION = "directories"

    LIB = "lib_dir"
    LOG = "log_dir"
    TMP = "tmp_dir"
    DATA = "data_dir"
    CONF = "conf_dir"

    KEYS = [LIB, LOG, TMP, DATA, CONF]

    def __init__(
        self,
        lib_dir: str = None,
        log_dir: str = None,
        tmp_dir: str = None,
        data_dir: str = None,
        conf_dir: str = None,
    ):
        self.lib = lib_dir
        self.log = log_dir
        self.tmp = tmp_dir
        self.data = data_dir
        self.conf = conf_dir

    # noinspection PyProtectedMember
    def __repr__(self) -> str:
        attrs = ["conf", "data", "tmp", "log", "lib"]
        return f"{type(self).__name__}({', '.join(f'{attr}={getattr(self, attr)._dir}' for attr in attrs)})"

    def __str__(self) -> str:
        attrs = ["conf", "data", "tmp", "log", "lib"]
        return f"[{self.SECTION}]\n" + "\n".join(f'{attr} = "{str(getattr(self, attr))}"' for attr in attrs)

    # noinspection PyProtectedMember
    def encode(self) -> Dict[str, Optional[str]]:
        dirs = {
            self.LIB: self._lib._dir,
            self.LOG: self._log._dir,
            self.TMP: self._tmp._dir,
            self.DATA: self._data._dir,
            self.CONF: self._conf._dir,
        }
        return dirs

    @property
    def lib(self) -> Directory:
        return self._lib

    # noinspection PyShadowingBuiltins
    @lib.setter
    def lib(self, dir: str) -> None:
        self._lib = Directory(dir, default="lib")

    @property
    def log(self) -> Directory:
        return self._log

    # noinspection PyShadowingBuiltins
    @log.setter
    def log(self, dir: str) -> None:
        self._log = Directory(dir, default="log")

    @property
    def tmp(self) -> Directory:
        return self._tmp

    # noinspection PyShadowingBuiltins
    @tmp.setter
    def tmp(self, dir: str) -> None:
        self._tmp = Directory(dir, default="tmp")

    @property
    def data(self) -> Directory:
        return self._data

    # noinspection PyShadowingBuiltins
    @data.setter
    def data(self, dir: str) -> None:
        self._data = Directory(dir, default="data")

    @property
    def conf(self) -> Directory:
        return self._conf

    # noinspection PyShadowingBuiltins
    @conf.setter
    def conf(self, dir: str) -> None:
        self._conf = Directory(dir, default="conf", base=self.data)

    # noinspection PyProtectedMember
    def copy(self) -> Directories:
        return Directories(
            self._lib._dir,
            self._log._dir,
            self._tmp._dir,
            self._data._dir,
            self._conf._dir
        )

    # noinspection PyProtectedMember, PyShadowingBuiltins
    def update(self, configs: Mapping[str, str]) -> None:
        for key in ["lib", "log", "tmp", "data"]:
            dir = configs.get(f"{key}_dir", None)
            if dir is not None:
                setattr(self, f"{key}", dir)
        conf_dir = configs.get("conf_dir", None)
        if conf_dir is not None:
            self.conf = conf_dir


class Directory(Path):
    _dir: Optional[Path] = None
    _base: Path
    default: str

    # noinspection PyShadowingBuiltins, PyTypeChecker
    def __new__(cls, *dirs: Optional[str], base: Optional[str | Directory] = None, default: Optional[str] = None):
        cls = WindowsDirectory if os.name == "nt" else PosixDirectory
        base = Directory.__parse_base(base)
        dir = Directory.__parse_dir(base, *dirs, default=default)
        return super().__new__(cls, Directory.__parse(base, dir, default), base=base, default=default)

    def __init__(self, *dirs: Optional[str], base: Optional[str | Directory] = None, default: Optional[str] = None):
        self.default = default
        self._base = Directory.__parse_base(base)
        self._dir = Directory.__parse_dir(self._base, *dirs, default=default)
        try:
            super().__init__(Directory.__parse(self._base, self._dir, default=default))

        except TypeError:
            # FixMe: The mro appears to be called incorrectly for python Versions < 3.12.
            # ToDo: Remove this catch, as older versions proceed to be deprecated.
            super().__init__()

    @staticmethod
    def __parse_base(base: Optional[str | Path]) -> Path:
        if base is None or (isinstance(base, Directory) and base.is_default()):
            base = Path.cwd()
        elif not isinstance(base, Path):
            base = Path(base)
        if base.is_relative_to("~"):
            base = base.expanduser()
        if not base.is_absolute():
            base = base.absolute()
        return base

    @staticmethod
    def __parse_dir(base: Path, *dirs: Optional[str], default: Optional[str] = None) -> Path:
        dir = Path(*dirs) if not any(d is None for d in dirs) else None
        if dir is not None:
            if dir.is_relative_to(base):
                dir = dir.relative_to(base)
            if dir.is_relative_to("~"):
                dir = dir.expanduser()
        if str(dir) == default:
            dir = None
        return dir

    @staticmethod
    def __parse(base: Path, path: Optional[Path], default: Optional[str] = None) -> str:
        if path is None:
            path = default
        if path is not None and not os.path.isabs(path):
            path = os.path.join(base, path)
        return str(path)

    def is_default(self) -> bool:
        return self._dir is None or str(self) == os.path.join(self._base, self.default)


class PosixDirectory(Directory, PosixPath):
    pass


class WindowsDirectory(Directory, WindowsPath):
    pass
