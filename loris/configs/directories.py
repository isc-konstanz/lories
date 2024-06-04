# -*- coding: utf-8 -*-
"""
    loris.configs.directories
    ~~~~~~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Dict, Optional


class Directories:
    SECTION = "directories"

    LIB = "lib_dir"
    LOG = "log_dir"
    TMP = "tmp_dir"
    DATA = "data_dir"
    CMPT = "cmpt_dir"
    CONF = "conf_dir"

    KEYS = [LIB, LOG, TMP, DATA, CMPT, CONF]

    def __init__(
        self,
        lib_dir: str = None,
        log_dir: str = None,
        tmp_dir: str = None,
        data_dir: str = None,
        cmpt_dir: str = None,
        conf_dir: str = None,
    ):
        self._run = os.getcwd()
        self._lib = lib_dir
        self._log = log_dir
        self._tmp = tmp_dir
        self._data = data_dir
        self._conf = conf_dir
        self._cmpt = cmpt_dir

    def __repr__(self):
        attrs = ["conf", "cmpt", "data", "tmp", "log", "lib"]
        return f"[{self.SECTION}]\n" + "\n".join(f"{attr}: {str(getattr(self, attr))}" for attr in attrs)

    def encode(self) -> Dict[str, Optional[str]]:
        dirs = {
            self.LIB: self._lib,
            self.LOG: self._log,
            self.TMP: self._tmp,
            self.DATA: self._data
        }
        if self._cmpt is None or not os.path.isabs(self._cmpt):
            dirs[self.CMPT] = self._cmpt
        if self._conf is None or not os.path.isabs(self._conf):
            dirs[self.CONF] = self._conf
        return dirs

    @property
    def lib(self):
        lib_dir = self._expand(self._lib) if self._lib is not None else "lib"
        if not os.path.isabs(lib_dir):
            lib_dir = os.path.join(self._run, lib_dir)
        return lib_dir

    @property
    def log(self):
        log_dir = self._expand(self._log) if self._log is not None else "log"
        if not os.path.isabs(log_dir):
            log_dir = os.path.join(self._run, log_dir)
        return log_dir

    @property
    def tmp(self):
        tmp_dir = self._expand(self._tmp) if self._tmp is not None else "tmp"
        if not os.path.isabs(tmp_dir):
            tmp_dir = os.path.join(self._run, tmp_dir)
        return tmp_dir

    @property
    def data(self):
        data_dir = self._expand(self._data) if self._data is not None else "data"
        if not os.path.isabs(data_dir):
            data_dir = os.path.join(os.getcwd(), data_dir)
        return data_dir

    @property
    def conf(self):
        conf_dir = self._expand(self._conf) if self._conf is not None else "conf"
        if not os.path.isabs(conf_dir):
            if self._data is None or self._run == os.path.dirname(self.data):
                conf_dir = os.path.join(self._run, conf_dir)
            else:
                conf_dir = os.path.join(self.data, conf_dir)
        return conf_dir

    @property
    def cmpt(self):
        if self._cmpt is not None:
            cmpt_dir = self._expand(self._cmpt)

            if not os.path.isabs(cmpt_dir):
                cmpt_dir = os.path.join(os.getcwd(), cmpt_dir)
        else:
            cmpt_dir = self.conf
            if os.path.isdir(os.path.join(self.conf, "core")):
                cmpt_dir = os.path.join(self.conf, "core")
        return cmpt_dir

    # noinspection PyShadowingBuiltins
    def join(self, configs: Mapping[str, str]) -> None:
        for key in ["lib", "log", "tmp", "data"]:
            dir = configs.get(f"{key}_dir")
            if dir is not None:
                setattr(self, f"_{key}", dir)

    # noinspection PyShadowingBuiltins
    @staticmethod
    def _expand(dir: str) -> str:
        if "~" in dir:
            return os.path.expanduser(dir)
        return dir
