# -*- coding: utf-8 -*-
"""
loris.core.context
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, Mapping, TypeVar

T = TypeVar("T")


class Context(ABC, Generic[T], Mapping[str, T]):

    def __getitem__(self, __uid: str) -> T:
        return self._get(__uid)

    def __setitem__(self, __uid: str, __value: T) -> None:
        self._set(__uid, __value)

    @abstractmethod
    def _get(self, __uid: str) -> T:
        pass

    @abstractmethod
    def _set(self, __uid: str, __value: T) -> None:
        pass

    @abstractmethod
    def _add(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def _new(self, *args, **kwargs) -> T:
        pass
