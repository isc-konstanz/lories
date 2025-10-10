# -*- coding: utf-8 -*-
"""
lori.core.resources
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Generic, Iterable, Iterator, List, Sequence, Tuple

from lori._core._resource import Resource  # noqa
from lori._core._resources import Resources as ResourcesType  # noqa
from lori._core._resources import _Resources  # noqa


class Resources(_Resources, Generic[Resource]):
    _resources: List[Resource]

    def __init__(self, resources=()) -> None:
        self._logger = logging.getLogger(type(self).__module__)
        self._resources = [*resources]

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(str(r.id) for r in self._resources)})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{r.id} = {repr(r)}" for r in self._resources)

    def __contains__(self, resource: str | Resource) -> bool:
        if isinstance(resource, str):
            return any(resource == r.id for r in self._resources)
        return resource in self._resources

    def __getitem__(self, index: Iterable[str] | str | int) -> Resource | ResourcesType:
        if isinstance(index, str):
            for resource in self._resources:
                if resource.id == index:
                    return resource
        if isinstance(index, Iterable):
            return type(self)([r for r in self._resources if r.id == index])
        raise KeyError(index)

    def __iter__(self) -> Iterator[Resource]:
        return iter(self._resources)

    def __len__(self) -> int:
        return len(self._resources)

    def __add__(self, other) -> ResourcesType:
        return type(self)([*self, *other])

    def append(self, resource: Resource) -> None:
        self._resources.append(resource)

    def extend(self, resources: Iterable[Resource]) -> None:
        self._resources.extend(resources)

    def update(self, resources: Iterable[Resource]) -> None:
        resource_ids = [r.id for r in resources]
        for resource in [r for r in self._resources if r.id in resource_ids]:
            self._resources.remove(resource)
        self._resources.extend(resources)

    @property
    def ids(self) -> Sequence[str]:
        return [str(resource.id) for resource in self._resources]

    @property
    def keys(self) -> Sequence[str]:
        return [str(resource.key) for resource in self._resources]

    def copy(self) -> ResourcesType:
        return type(self)([resource.copy() for resource in self._resources])

    def apply(self, apply: Callable[[Resource], Resource], inplace: bool = False) -> ResourcesType:
        resources = self._resources if not inplace else self._resources.copy()
        return type(self)([apply(resource) for resource in resources])

    def filter(self, *filters: Callable[[Resource], bool]) -> ResourcesType:
        def _filters(channel: Resource) -> bool:
            for _filter in filters:
                if _filter is not None and not _filter(channel):
                    return False
            return True

        return type(self)([resource for resource in self._resources if _filters(resource)])

    # noinspection PyShadowingBuiltins, SpellCheckingInspection
    def groupby(self, by: Callable[[Resource], Any] | str) -> Iterator[Tuple[Any, ResourcesType]]:
        def _by(r: Resource) -> Any:
            return r.get(by, default=None)

        filter = _by if isinstance(by, str) else by
        for group_by in list(dict.fromkeys([filter(r) for r in self])):
            yield group_by, self.filter(lambda r: filter(r) == group_by)

    # noinspection PyTypeChecker
    def to_configs(self) -> Dict[str, Any]:
        return {r.id: r.to_configs() for r in self._resources}
