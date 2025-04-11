# -*- coding: utf-8 -*-
"""
lori.core.constant
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Dict, Optional, Type

from lori.core import ResourceException
from lori.util import parse_type


# noinspection PyPep8Naming, PyShadowingBuiltins
class Constant(str):
    def __new__(cls, type: Type, key: str, name: Optional[str] = None, unit: Optional[str] = None):
        from lori.core.constants import CONSTANTS

        if key in CONSTANTS:
            raise ResourceException(f"Constant '{key}' already exists.")
        constant = str.__new__(cls, key)
        CONSTANTS.append(constant)
        return constant

    def __init__(self, type: Type | str, key: str, name: Optional[str] = None, unit: Optional[str] = None) -> None:
        self.__type = parse_type(type)
        self.__key = key
        self.__name = name
        self.__unit = unit

    def __str__(self) -> str:
        return self.key

    @property
    def type(self) -> Type:
        return self.__type

    @property
    def key(self) -> str:
        return self.__key

    @property
    def name(self) -> str:
        return self.__name

    @property
    def unit(self) -> Optional[str]:
        return self.__unit

    def full_name(self, unit: bool = False) -> str:
        name = self.name
        if unit and self.unit is not None and len(self.unit) > 0:
            name += f" [{self.unit}]"
        return name

    def to_dict(self) -> Dict[str, Any]:
        dict = {
            "type": self.__type,
            "key": self.__key,
            "name": self.__name if self.__name is not None else self.__key.replace("_", " ").title(),
        }
        if self.__unit is not None:
            dict["unit"] = self.__unit
        return dict
