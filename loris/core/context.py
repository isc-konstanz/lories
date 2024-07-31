# -*- coding: utf-8 -*-
"""
loris.core.context
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, Iterator, Mapping, TypeVar

C = TypeVar("C")


class Context(ABC, Generic[C], Mapping[str, C]):
    @abstractmethod
    def __iter__(self) -> Iterator[str]:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def __contains__(self, *args) -> bool:
        pass

    def __getitem__(self, *args) -> C:
        return self._get(*args)

    @abstractmethod
    def _get(self, *args) -> C:
        pass

    @abstractmethod
    def _set(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def _add(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def _new(self, *args, **kwargs) -> C:
        pass

    # noinspection PyShadowingBuiltins
    @abstractmethod
    def _remove(self, id: str, **kwargs) -> None:
        pass
