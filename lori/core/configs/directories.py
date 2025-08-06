# -*- coding: utf-8 -*-
"""
lori.core.configs.directories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path, PosixPath, WindowsPath
from typing import Dict, Optional

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import LiteralString

except ImportError:
    from typing_extensions import LiteralString


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

    def __copy__(self) -> Directories:
        return self.copy()

    def __deepcopy__(self, memo) -> Directories:
        return self.__copy__()

    # noinspection PyProtectedMember
    def to_dict(self) -> Dict[str, Optional[str]]:
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
    def lib(self, dir: str | Directory) -> None:
        if isinstance(dir, Directory):
            dir = str(dir)
        self._lib = Directory(dir, default="lib")

    @property
    def log(self) -> Directory:
        return self._log

    # noinspection PyShadowingBuiltins
    @log.setter
    def log(self, dir: str | Directory) -> None:
        if isinstance(dir, Directory):
            dir = str(dir)
        self._log = Directory(dir, default="log")

    @property
    def tmp(self) -> Directory:
        return self._tmp

    # noinspection PyShadowingBuiltins
    @tmp.setter
    def tmp(self, dir: str | Directory) -> None:
        if isinstance(dir, Directory):
            dir = str(dir)
        self._tmp = Directory(dir, default="tmp")

    @property
    def data(self) -> Directory:
        return self._data

    # noinspection PyShadowingBuiltins
    @data.setter
    def data(self, dir: str | Directory) -> None:
        if isinstance(dir, Directory):
            dir = str(dir)
        self._data = Directory(dir, default="data")

    @property
    def conf(self) -> Directory:
        return self._conf

    # noinspection PyShadowingBuiltins
    @conf.setter
    def conf(self, dir: str | Path) -> None:
        if isinstance(dir, Path):
            dir = str(dir)
        self._conf = Directory(dir, default="conf", base=self.data)

    # noinspection PyProtectedMember
    def copy(self) -> Directories:
        return Directories(
            self._lib._dir,
            self._log._dir,
            self._tmp._dir,
            self._data._dir,
            self._conf._dir,
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
    def __new__(cls, *dirs: str, base: Optional[str | Directory] = None, default: Optional[str] = None):
        cls = WindowsDirectory if os.name == "nt" else PosixDirectory
        base = Directory.__parse_base(base)
        dir = Directory.__parse_dir(base, *dirs, default=default)
        return super().__new__(cls, Directory.__parse(base, dir, default), base=base, default=default)

    def __init__(self, *dirs: str, base: Optional[str | Directory] = None, default: Optional[str] = None):
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
    def __parse_base(base: Optional[LiteralString | str | Path]) -> Path:
        if base is None or (isinstance(base, Directory) and base.is_default()):
            base = Path.cwd()
        elif isinstance(base, Directory) or not isinstance(base, Path):
            base = Path(base)
        # FIXME: Remove this once Python >= 3.9 is a requirement
        # if base.is_relative_to("~"):
        if str(base).startswith("~"):
            base = base.expanduser()
        if not base.is_absolute():
            base = base.absolute()
        return base

    # noinspection PyShadowingBuiltins
    @staticmethod
    def __parse_dir(base: Path, *dirs: Optional[str], default: Optional[str] = None) -> Path:
        dir = Path(*dirs) if not any(d is None for d in dirs) else None
        if dir is not None:
            # FIXME: Remove this once Python >= 3.9 is a requirement
            # if dir.is_relative_to(base):
            if str(dir).startswith(str(base)):
                dir = dir.relative_to(base)
            # if dir.is_relative_to("~"):
            if str(dir).startswith("~"):
                dir = dir.expanduser()
        if str(dir) == default:
            dir = None
        return dir

    @staticmethod
    def __parse(base: Path, path: Optional[Path], default: Optional[str] = None) -> Path:
        if path is None:
            path = Path(default)
        if path is not None and not os.path.isabs(path):
            path = base.joinpath(path)
        return path

    def is_default(self) -> bool:
        if self.default is None:
            return False
        return self._dir is None or str(self) == os.path.join(self._base, self.default)

    # noinspection SpellCheckingInspection
    def joinpath(self, *paths: LiteralString | str | Path) -> Directory:
        _dir = self.__parse(self._base, self._dir, self.default)
        return Directory(_dir.joinpath(*paths), default=self.default, base=self._base)

    def relative_to(self, path: LiteralString | str | Path, *_, walk_up=False) -> Path:
        return self.__parse(self._base, self._dir, self.default).relative_to(path)


class PosixDirectory(Directory, PosixPath):
    pass


class WindowsDirectory(Directory, WindowsPath):
    pass
