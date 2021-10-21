# -*- coding: utf-8 -*-
"""
    th-e-core.tools
    ~~~~~~~~~~~~~~~
    
    
"""
import os
from configparser import ConfigParser


def _path(configs: ConfigParser,
          key: str,
          path: str,
          section: str = 'General') -> str:
    if configs.has_option(section, key):
        path = configs.get(section, key)

    if "~" in path:
        path = os.path.expanduser(path)

    if not os.path.isabs(path) and \
            configs.has_option(section, 'root_dir'):
        base = configs.get(section, 'root_dir')
        path = os.path.join(base, path)

    return path


def _bool(v: object) -> bool:
    if isinstance(v, str):
        return v.lower() == 'true'

    return v


def _float(v: object) -> float:
    if isinstance(v, str):
        return float(v)

    return v


def _int(v: object) -> int:
    if isinstance(v, str):
        return int(v)

    return v
