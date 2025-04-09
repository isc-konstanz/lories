# -*- coding: utf-8 -*-
"""
lori.core.constants
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import ABCMeta
from collections.abc import Callable
from typing import Iterable, Iterator, List, MutableSequence

from lori.core.constant import Constant


class _ConstantsMeta(ABCMeta):
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(_ConstantsMeta, cls).__call__(*args, **kwargs)
        return cls._instance


# noinspection PyShadowingNames
class _Constants(MutableSequence[Constant], metaclass=_ConstantsMeta):
    _constants: List[Constant]

    def __init__(self, constants=()) -> None:
        self._constants = [*constants]

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(str(c) for c in self._constants)})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{c.key} = {repr(c)}" for c in self._constants)

    def __contains__(self, constant: str | Constant) -> bool:
        if isinstance(constant, str):
            return any(constant == r.key for r in self._constants)
        return constant in self._constants

    def __getitem__(self, index: Iterable[str] | str | int):
        if isinstance(index, str):
            for constant in self._constants:
                if constant.key == index:
                    return constant
        if isinstance(index, Iterable):
            return type(self)([r for r in self._constants if r.key == index])
        raise KeyError(index)

    def __setitem__(self, index: int, constant: Constant) -> None:
        self._constants[index] = constant

    def __delitem__(self, index: int) -> None:
        del self._constants[index]

    def __iter__(self) -> Iterator[Constant]:
        return iter(self._constants)

    def __len__(self) -> int:
        return len(self._constants)

    def __add__(self, other):
        return type(self)([*self, *other])

    def insert(self, index: int, constant: Constant):
        self._constants.insert(index, constant)

    def extend(self, constants: Iterable[Constant]) -> None:
        self._constants.extend(constants)

    # noinspection PyShadowingBuiltins
    def filter(self, filter: Callable[[Constant], bool]):
        return type(self)([constant for constant in self._constants if filter(constant)])


CONSTANTS = _Constants()

SECOND = Constant(int, "second", "Second")
MINUTE = Constant(int, "minute", "Minute")
HOUR = Constant(int, "hour", "Hour")
DAY_OF_WEEK = Constant(int, "day_of_week", "Day of the Week")
DAY_OF_YEAR = Constant(int, "day_of_year", "Day of the Year")
MONTH = Constant(int, "month", "Month")
YEAR = Constant(int, "year", "Year")
