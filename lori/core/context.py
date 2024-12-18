# -*- coding: utf-8 -*-
"""
lori.core.context
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import re
from collections import OrderedDict
from collections.abc import Callable, Collection, MutableMapping
from itertools import chain
from typing import Any, Generic, Iterator, Tuple, TypeVar

import pandas as pd
from lori.core import Identifier

ID = TypeVar("ID", bound=Identifier)


class Context(Generic[ID], MutableMapping[str, ID]):
    __map: OrderedDict[str, ID]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__map = OrderedDict[str, ID]()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(str(c.id) for c in self.__map.values())})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{i} = {repr(c)}" for i, c in self.__map.items())

    def __iter__(self) -> Iterator[str]:
        return iter(self.__map)

    def __len__(self) -> int:
        return len(self.__map)

    def __contains__(self, __object: str | ID) -> bool:
        if isinstance(__object, str):
            return __object in self.__map.keys()
        if isinstance(__object, Identifier):
            return __object in self.__map.values()
        return False

    def __getitem__(self, __uid: str) -> ID:
        return self._get(__uid)

    def __setitem__(self, __uid: str, __object: ID) -> None:
        self._set(__uid, __object)

    def __delitem__(self, __uid: str) -> None:
        del self.__map[__uid]

    def _get(self, __uid: str) -> ID:
        return self.__map[__uid]

    def _set(self, __uid: str, __object: ID) -> None:
        self.__map[__uid] = __object

    def _add(self, *__objects: ID) -> None:
        for __object in __objects:
            self._set(__object.id, __object)

    # noinspection PyShadowingBuiltins
    def filter(self, filter: Callable[[ID], bool]) -> Collection[ID]:
        return [c for c in self.__map.values() if filter(c)]

    def sort(self):
        def order(text: str) -> Tuple[Any, ...]:
            elements = re.split(r"[^0-9A-Za-zäöüÄÖÜß]+", text)
            elements = list(chain(*[re.split(r"([0-9])+", t) for t in elements]))
            elements = [int(t) if t.isdigit() else t for t in elements if pd.notna(t) and t.strip()]
            return tuple(elements)

        self.__map = OrderedDict(sorted(self.__map.items(), key=lambda e: order(e[0])))
