# -*- coding: utf-8 -*-
"""
    th-e-core.tools
    ~~~~~~~~~~~~~~~
    
    
"""


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
