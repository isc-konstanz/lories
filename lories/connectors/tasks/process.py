# -*- coding: utf-8 -*-
"""
lories.data.manager
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import multiprocessing as mp
import os
from collections.abc import Callable
from concurrent.futures import Executor, ProcessPoolExecutor
from typing import Collection, Optional, Sequence

from lories._core._channel import Channel  # noqa
from lories._core._channels import ChannelsArgument  # noqa
from lories._core._connector import Connector  # noqa
from lories._core._registrator import Registrator  # noqa
from lories.connectors import ConnectorContext
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

    def _build(self, configs: Configurations) -> Executor:
        return ProcessPoolExecutor(
            max_workers=configs.get_int("workers_max", default=max(int((os.cpu_count() or 1) / 2), 1)),
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
