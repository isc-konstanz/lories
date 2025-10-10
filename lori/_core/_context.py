# -*- coding: utf-8 -*-
"""
lori._core._context
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Callable, MutableMapping
from itertools import chain
from typing import Any, Collection, Generic, Iterable, Iterator, Optional, Sequence, Tuple, TypeVar, overload

import pandas as pd
from lori._core._entity import Entity, _Entity


# noinspection PyAbstractClass
class _Context(ABC, Generic[Entity], MutableMapping[str, Entity]):
    __map: OrderedDict[str, Entity]

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

    def __contains__(self, __object: str | Entity) -> bool:
        return self._contains(__object)

    def __getitem__(self, __uid: Iterable[str] | str) -> Entity | Collection[Entity]:
        if isinstance(__uid, str):
            return self._get(__uid)
        if isinstance(__uid, Iterable):
            return [self._get(i) for i in __uid]
        raise KeyError(__uid)

    def __setitem__(self, __uid: str, __object: Entity) -> None:
        self._set(__uid, __object)

    def __delitem__(self, __uid: str) -> None:
        self._remove(__uid)

    def _contains(self, __object: str | Entity) -> bool:
        if isinstance(__object, str):
            return __object in self.__map.keys()
        if isinstance(__object, _Entity):
            return __object in self.__map.values()
        return False

    def _get(self, __uid: str) -> Entity:
        return self.__map[__uid]

    def _set(self, __uid: str, __object: Entity) -> None:
        if __uid in self.keys():
            raise ValueError(f'Entity with ID "{__uid}" already exists')

        self.__map[__uid] = __object

    def _add(self, *__objects: Entity) -> None:
        for __object in __objects:
            self._set(str(__object.id), __object)

    @abstractmethod
    def _create(self, *args, **kwargs) -> Entity: ...

    @abstractmethod
    def _update(self, *args, **kwargs) -> None: ...

    def _remove(self, *__objects: str | Entity) -> None:
        for __object in __objects:
            if isinstance(__object, str):
                del self.__map[__object]
            elif isinstance(__object, _Entity):
                del self.__map[__object.id]

    def sort(self):
        def order(text: str) -> Tuple[Any, ...]:
            elements = re.split(r"[^0-9A-Za-zäöüÄÖÜß]+", text)
            elements = list(chain(*[re.findall(r"\D+|\d+", t) for t in elements]))
            elements = [(int(t), 0) if t.isdigit() else (0, t) for t in elements if pd.notna(t) and t.strip()]
            return tuple(elements)

        self.__map = OrderedDict(sorted(self.__map.items(), key=lambda e: order(e[0])))

    # noinspection PyShadowingBuiltins
    @overload
    def filter(self, filter: Optional[Callable[[Entity], bool]]) -> Sequence[Entity]: ...

    def filter(self, *filters: Optional[Callable[[Entity], bool]]) -> Sequence[Entity]:
        def _filters(__object: Entity) -> bool:
            for _filter in filters:
                if _filter is not None and not _filter(__object):
                    return False
            return True

        return [c for c in self.__map.values() if _filters(c)]


Context = TypeVar("Context", bound=_Context)
