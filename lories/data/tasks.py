# -*- coding: utf-8 -*-
"""
lories.data.tasks
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import os
from abc import abstractmethod
from collections.abc import Callable
from concurrent import futures
from concurrent.futures import Executor, Future, ThreadPoolExecutor, TimeoutError
from typing import Any, Dict, Optional, Sequence

import pandas as pd
from lories._core import _TaskContext  # noqa
from lories.connectors import Connector, ConnectorError
from lories.connectors.tasks import CheckTask, LogTask, ReadTask, WriteTask
from lories.core.activator import Activator
from lories.core.errors import ResourceError, ResourceUnavailableError
from lories.core.typing import Channel, Channels, Configurations, Registrator, Timestamp
from lories.data.channels import ChannelState


# noinspection PyShadowingBuiltins
class TaskContext(_TaskContext, Activator):
    TYPE: str = "tasks"

    _task_prefix: str = ""

    __executor: Optional[Executor] = None

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._task_prefix = kwargs.get("name", "")

    def __eq__(self, other: Any) -> bool:
        return self is other

    def __hash__(self) -> int:
        return hash(id(self))

    def __getstate__(self) -> Dict[str, Any]:
        state = super().__getstate__()
        state.pop(f"{_TaskContext.__name__}__executor", None)
        return state

    def _submit(self, fn, /, *args, **kwargs) -> Future:
        if self.__executor is None:
            raise ResourceUnavailableError("Executor not initialized")

        return self.__executor.submit(fn, *args, **kwargs)

    def _build(self, configs: Configurations) -> Executor:
        get_cpu_count = getattr(os, "process_cpu_count", os.cpu_count)
        max_workers_default = min(32, (get_cpu_count() or 1) + 4)
        return ThreadPoolExecutor(
            thread_name_prefix=self._task_prefix,
            max_workers=configs.get_int("workers_max", default=max_workers_default),
        )

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        if self.is_enabled():
            self.__executor = self._build(configs.get_member(TaskContext.TYPE, defaults={}))

    def activate(self) -> None:
        super().activate()

    def deactivate(self, *_) -> None:
        self.interrupt()
        super().deactivate()

    def interrupt(self, *_) -> None:
        if self.__executor is not None:
            self.__executor.shutdown(wait=True, cancel_futures=True)

    def terminate(self, *_) -> None:
        self.interrupt()

    def has_logged(self, channels: Channels, **kwargs) -> bool:
        if channels is None:
            raise ResourceError("Missing required 'channels' argument to check for logged data")
        return self._has_logged(channels, **kwargs)

    def _has_logged(
        self,
        channels: Channels,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> bool:
        check_futures = {}
        for connector in self._filter_connected():
            # noinspection PyUnresolvedReferences
            def has_database(channel: Channel) -> bool:
                return channel.has_logger(connector.id) and channel.logger.is_database()

            check_channels = channels.filter(has_database).apply(lambda c: c.from_logger())
            if len(check_channels) == 0:
                continue

            check_task = CheckTask(connector, check_channels)
            check_future = self._submit(check_task, start=start, end=end)
            check_futures[check_future] = check_task

        check_results = []
        try:
            for check_future in futures.as_completed(check_futures, timeout=timeout):
                check_task = check_futures.pop(check_future)
                try:
                    check_exists = check_future.result()
                    check_results.append(check_exists)

                except ConnectorError as e:
                    self._logger.warning(f"Failed checking connector '{check_task.connector.id}': {str(e)}")
                    if self._logger.getEffectiveLevel() <= logging.DEBUG:
                        self._logger.exception(e)

                    check_results.append(False)

        except TimeoutError:
            for check_future, check_task in check_futures.items():
                self._logger.warning(
                    f"Timed out checking connector '{check_task.connector.id}' after {timeout} seconds"
                )
                check_future.cancel()
                check_results.append(False)

        if len(check_results) == 0:
            return False
        return all(check_results)

    def read_logged(self, channels: Channels, **kwargs) -> pd.DataFrame:
        if channels is None:
            raise ResourceError("Missing required 'channels' argument to read logged data")
        return self._read_logged(channels, **kwargs)

    def _read_logged(
        self,
        channels: Channels,
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
    ) -> pd.DataFrame:
        read_futures = {}
        for connector in self._filter_connected():
            # noinspection PyUnresolvedReferences
            def has_database(channel: Channel) -> bool:
                return channel.has_logger(connector.id) and channel.logger.is_database()

            read_channels = channels.filter(has_database).apply(lambda c: c.from_logger())
            if len(read_channels) == 0:
                continue

            read_task = ReadTask(connector, read_channels)
            read_future = self._submit(read_task, start=start, end=end)
            read_futures[read_future] = read_task

        return self._read_futures(read_futures, timeout)

    def read(self, channels: Channels, **kwargs) -> pd.DataFrame:
        if channels is None:
            raise ResourceError("Missing required 'channels' argument to read data")
        return self._read(channels, **kwargs)

    # noinspection PyTypeChecker
    def _read(
        self,
        channels: Channels,
        timeout: Optional[float] = None,
        inplace: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        read_futures = {}
        for connector in self._filter_connected():
            read_channels = channels.filter(lambda c: c.has_connector(connector.id))
            if len(read_channels) == 0:
                continue

            read_task = ReadTask(connector, read_channels)
            read_future = self._submit(read_task, inplace=inplace, **kwargs)
            read_futures[read_future] = read_task

        return self._read_futures(read_futures, timeout, inplace)

    def _read_callback(
        self,
        task: ReadTask,
        future: Future,
        inplace: bool = False,
    ) -> Optional[pd.DataFrame]:
        channels = task.channels
        try:
            return future.result()

        except ConnectorError as e:
            self._logger.warning(f"Failed reading connector '{task.connector.id}': {str(e)}")
            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.exception(e)
            if inplace:
                channels.set_state(ChannelState.READ_ERROR)
        return None

    def _read_futures(
        self,
        tasks: Dict[Future, ReadTask],
        timeout: Optional[float] = None,
        inplace: bool = False,
    ) -> pd.DataFrame:
        results = []
        try:
            for future in futures.as_completed(tasks, timeout=timeout):
                task = tasks.pop(future)
                data = self._read_callback(task, future, inplace)
                if data is not None:
                    results.append(data)

        except TimeoutError:
            for future, task in tasks.items():
                self._logger.warning(f"Timed out reading connector '{task.connector.id}' after {timeout} seconds")
                future.cancel()
                if inplace:
                    channels = task.channels
                    channels.set_state(ChannelState.TIMEOUT)

        if len(results) == 0:
            return pd.DataFrame()
        results = sorted(results, key=lambda d: min(d.index))
        return pd.concat(results, axis="columns")

    def write(self, data: pd.DataFrame, channels: Channels, **kwargs) -> None:
        if channels is None:
            raise ResourceError("Missing required 'channels' argument to write data")
        return self._write(data, channels, **kwargs)

    # noinspection PyShadowingBuiltins, PyShadowingNames, PyTypeChecker
    def _write(
        self,
        data: pd.DataFrame,
        channels: Channels,
        timeout: Optional[float] = None,
        inplace: bool = False,
    ) -> None:
        write_futures = {}
        for connector in self._filter_connected():
            write_channels = channels.filter(lambda c: (c.has_connector(connector.id) and c.id in data.columns))
            if len(write_channels) == 0:
                continue

            write_channels.set_frame(data)
            write_task = WriteTask(connector, write_channels)
            write_future = self._submit(write_task)
            write_futures[write_future] = write_task

        self._write_futures(write_futures, timeout, inplace=inplace)

    def _write_futures(
        self,
        tasks: Dict[Future, WriteTask | LogTask],
        timeout: Optional[float] = None,
        inplace: bool = False,
    ) -> None:
        try:
            for future in futures.as_completed(tasks, timeout=timeout):
                task = tasks.pop(future)
                self._write_callback(task, future, inplace)

        except TimeoutError:
            for future, task in tasks.items():
                self._logger.warning(f"Timed out writing connector '{task.connector.id}' after {timeout} seconds")
                future.cancel()
                if inplace:
                    channels = task.channels
                    channels.set_state(ChannelState.TIMEOUT)

    def _write_callback(
        self,
        task: WriteTask,
        future: Future,
        inplace: bool = False,
    ) -> None:
        channels = task.channels
        try:
            future.result()

        except ConnectorError as e:
            self._logger.warning(f"Failed writing connector '{task.connector.id}': {str(e)}")
            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.exception(e)
            if inplace:
                channels.set_state(ChannelState.WRITE_ERROR)

    # noinspection PyProtectedMember
    def _filter_connected(self) -> Sequence[Connector]:
        return self._filter_connectors(lambda c: c._is_connected())

    @abstractmethod
    def _filter_connectors(self, *filters: Optional[Callable[[Connector], bool]]) -> Sequence[Connector]: ...


def chain_filters(*filters: Optional[Callable[[Registrator], bool]]) -> Callable[[...], bool]:
    def _all_filters(registrator: Registrator) -> bool:
        return all(f(registrator) for f in filters if f is not None)

    return _all_filters
