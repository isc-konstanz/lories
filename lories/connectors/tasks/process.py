# -*- coding: utf-8 -*-
"""
lories.data.manager
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import multiprocessing as mp
import os
from collections.abc import Callable
from concurrent.futures import Executor, Future, ProcessPoolExecutor
from multiprocessing.reduction import ForkingPickler
from typing import Collection, Optional, Sequence

from lories._core._channel import Channel  # noqa
from lories._core._channels import ChannelsArgument  # noqa
from lories._core._connector import Connector  # noqa
from lories._core._registrator import Registrator  # noqa
from lories.connectors import ConnectorContext
from lories.core.errors import ResourceError
from lories.core.typing import Configurations
from lories.data.converters import ConverterContext
from lories.data.processors import ProcessorContext
from lories.data.tasks import TaskContext


# noinspection PyProtectedMember
class ProcessContext(TaskContext):
    _context: mp.context.BaseContext
    _connectors: ConnectorContext
    _converters: ConverterContext

    def __init__(
        self,
        configs: Optional[Configurations] = None,
        connectors: Collection[Connector] = (),
        *args,
        **kwargs,
    ) -> None:
        super().__init__(configs=configs, *args, **kwargs)
        self._context = mp.get_context("spawn")
        self._processors = ProcessorContext()
        self._converters = ConverterContext(self)
        self._connectors = ConnectorContext(self)
        self._connectors._add(*(connector.duplicate(context=self._connectors) for connector in connectors))

    def _build(self) -> Executor:
        get_cpu_count = getattr(os, "process_cpu_count", os.cpu_count)
        max_workers_default = max(int((get_cpu_count() or 1) / 2), 1)
        workers_max = (self._tasks_section or {}).get("workers_max")
        return ProcessPoolExecutor(
            max_workers=workers_max if workers_max is not None else max_workers_default,
            mp_context=self._context,
        )

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._converters.load(configs.get_member(ConverterContext.TYPE, ensure_exists=True))
        self._connectors.load(configs.get_member(ConnectorContext.TYPE, ensure_exists=True))

    # noinspection PyShadowingBuiltins
    def activate(self) -> None:
        super().activate()
        self._connectors.connect()

    # noinspection PyShadowingBuiltins
    def deactivate(self) -> None:
        super().deactivate()
        self._connectors.disconnect()

    @property
    def processors(self) -> ProcessorContext:
        return self._processors

    @property
    def converters(self) -> ConverterContext:
        return self._converters

    @property
    def connectors(self) -> ConnectorContext:
        return self._connectors

    def _filter_connectors(self, *filters: Optional[Callable[[Connector], bool]]) -> Sequence[Connector]:
        return self._connectors.filter(*filters)

    def _submit(self, fn, /, *args, **kwargs) -> Future:
        for arg in [*args, *kwargs.values()]:
            result = assert_picklable(arg)
            if result:
                raise ResourceError(f"Argument {result[0]} of type {result[1]} is not picklable: {result[2]}")
        return super()._submit(fn, *args, **kwargs)


def assert_picklable(obj, path="root", max_depth=6, max_items=50, seen=None):
    if seen is None:
        seen = set()
    xid = id(obj)
    if xid in seen:
        return None
    seen.add(xid)
    try:
        ForkingPickler.dumps(obj)
        return None

    except Exception as e:
        if max_depth <= 0:
            return path, type(obj), repr(e)

        if isinstance(obj, dict):
            for i, (k, v) in enumerate(list(obj.items())[:max_items]):
                r = assert_picklable(k, f"{path}[key#{i}]", max_depth - 1, max_items, seen)
                if r:
                    return r
                r = assert_picklable(v, f"{path}[{repr(k)[:40]}]", max_depth - 1, max_items, seen)
                if r:
                    return r
            return path, type(obj), repr(e)

        if isinstance(obj, (list, tuple, set)):
            for i, v in enumerate(list(obj)[:max_items]):
                r = assert_picklable(v, f"{path}[{i}]", max_depth - 1, max_items, seen)
                if r:
                    return r
            return path, type(obj), repr(e)

        d = getattr(obj, "__dict__", None)
        if isinstance(d, dict):
            for k, v in list(d.items())[:max_items]:
                r = assert_picklable(v, f"{path}.{k}", max_depth - 1, max_items, seen)
                if r:
                    return r

        return path, type(obj), repr(e)
