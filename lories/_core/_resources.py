# -*- coding: utf-8 -*-
"""
lories._core._resources
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Generic, Iterable, Iterator, Sequence, Tuple, TypeVar, Union, overload

from lories._core._resource import Resource, _Resource


class _Resources(ABC, Generic[Resource], Sequence[Resource]):
    @abstractmethod
    def append(self, resource: Resource) -> None: ...

    @abstractmethod
    def extend(self, resources: Iterable[Resource]) -> None: ...

    @abstractmethod
    def update(self, resources: Iterable[Resource]) -> None: ...

    @abstractmethod
    def ids(self) -> Sequence[str]: ...

    @abstractmethod
    def keys(self) -> Sequence[str]: ...

    @abstractmethod
    def copy(self) -> Resources: ...

    @abstractmethod
    def apply(self, apply: Callable[[Resource], Resource], inplace: bool = False): ...

    # noinspection PyShadowingBuiltins
    @overload
    def filter(self, filter: Callable[[Resource], bool]): ...

    @abstractmethod
    def filter(self, *filters: Callable[[Resource], bool]): ...

    # noinspection PyShadowingBuiltins, SpellCheckingInspection
    @abstractmethod
    def groupby(self, by: Callable[[Resource], Any] | str) -> Iterator[Tuple[Any, Resources]]: ...

    @abstractmethod
    def to_configs(self) -> Dict[str, Any]: ...


Resources = TypeVar("Resources", bound=_Resources[_Resource])

ResourcesArgument = TypeVar(
    "ResourcesArgument",
    bound=Union[_Resource, _Resources[_Resource], Iterable[_Resource], Iterable[str], str],
)
