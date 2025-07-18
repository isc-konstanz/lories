# -*- coding: utf-8 -*-
"""
lori.core.context
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Callable, MutableMapping
from itertools import chain
from typing import Any, Collection, Generic, Iterable, Iterator, Tuple, TypeVar

import pandas as pd
from lori.core import Entity, ResourceException

E = TypeVar("E", bound=Entity)


# noinspection PyAbstractClass
class Context(ABC, MutableMapping[str, E], Generic[E]):
    __map: OrderedDict[str, E]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__map = OrderedDict()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(str(c.id) for c in self.__map.values())})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{i} = {repr(c)}" for i, c in self.__map.items())

    def __iter__(self) -> Iterator[str]:
        return iter(self.__map.keys())

    def __len__(self) -> int:
        return len(self.__map)

    def __contains__(self, __object: str | E) -> bool:
        return self._contains(__object)

    def __getitem__(self, __uid: Iterable[str] | str) -> E | Collection[E]:
        if isinstance(__uid, str):
            return self._get(__uid)
        if isinstance(__uid, Iterable):
            return [self._get(i) for i in __uid]
        raise KeyError(__uid)

    def __setitem__(self, __uid: str, __object: E) -> None:
        self._set(__uid, __object)

    def __delitem__(self, __uid: str) -> None:
        self._remove(__uid)

    def _contains(self, __object: str | E) -> bool:
        if isinstance(__object, str):
            return __object in self.__map.keys()
        if isinstance(__object, Entity):
            return __object in self.__map.values()
        return False

    def _get(self, __uid: str) -> E:
        return self.__map[__uid]

    def _set(self, __uid: str, __object: E) -> None:
        if id in self.keys():
            raise ResourceException(f'Entity with ID "{__uid}" already exists')

        self.__map[__uid] = __object

    def _add(self, *__objects: E) -> None:
        for __object in __objects:
            self._set(str(__object.id), __object)

    @abstractmethod
    def _create(self, *args, **kwargs) -> E: ...

    @abstractmethod
    def _update(self, *args, **kwargs) -> None: ...

    def _remove(self, *__objects: str | E) -> None:
        for __object in __objects:
            if isinstance(__object, str):
                del self.__map[__object]
            if isinstance(__object, Entity):
                del self.__map[__object.id]

    def sort(self):
        def order(text: str) -> Tuple[Any, ...]:
            elements = re.split(r"[^0-9A-Za-zäöüÄÖÜß]+", text)
            elements = list(chain(*[re.findall(r"\D+|\d+", t) for t in elements]))
            elements = [int(t) if t.isdigit() else t for t in elements if pd.notna(t) and t.strip()]
            return tuple(elements)

        self.__map = OrderedDict(sorted(self.__map.items(), key=lambda e: order(e[0])))

    # noinspection PyShadowingBuiltins
    def filter(self, filter: Callable[[E], bool]) -> Collection[E]:
        return [c for c in self.__map.values() if filter(c)]
