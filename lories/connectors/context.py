# -*- coding: utf-8 -*-
"""
lories.connectors.context
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from concurrent import futures
from concurrent.futures import Future
from typing import Callable, Dict, Optional, Type

from lories._core._channels import Channels  # noqa
from lories._core._connector import Connector, _Connector, _ConnectorContext  # noqa
from lories.connectors.errors import ConnectorError
from lories.connectors.tasks import ConnectTask
from lories.core.configs import Configurations
from lories.core.register import RegistratorContext, Registry
from lories.data.channels import ChannelState

registry = Registry[_Connector]()


# noinspection PyShadowingBuiltins
def register_connector_type(
    type: str,
    *alias: str,
    factory: Callable[[ConnectorContext | Connector, Optional[Configurations]], Connector] = None,
    replace: bool = False,
) -> Callable[[Type[Connector]], Type[Connector]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[Connector]) -> Type[Connector]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


# noinspection PyProtectedMember
class ConnectorContext(_ConnectorContext, RegistratorContext[Connector]):
    @property
    def _registry(self) -> Registry[Connector]:
        return registry

    # noinspection PyShadowingBuiltins
    def connect(
        self,
        filter: Optional[Callable[[Connector], bool]] = None,
        channels: Optional[Channels] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self._connect(*self.filter(filter), channels=channels, timeout=timeout)

    def _connect(
        self,
        *connectors: Connector,
        channels: Optional[Channels] = None,
        timeout: Optional[float] = None,
    ) -> None:
        connect_futures = {}
        for connector in connectors:
            if not connector.is_enabled():
                self._logger.debug(
                    f"Skipping to connect disabled {type(connector).__name__} '{connector.name}': {connector.id}"
                )
                continue

            if connector._is_connected():
                self._logger.debug(
                    f"Skipping already connected {type(connector).__name__} '{connector.name}': {connector.id}"
                )
                continue

            if not connector._is_connectable():
                self._logger.debug(
                    f"Skipping not connectable {type(connector).__name__} '{connector.name}': {connector.id}"
                )
                continue

            connect_task = self.__connect(connector, channels)
            connect_future = self._executor.submit(connect_task)
            connect_futures[connect_future] = connect_task

        self.__connect_futures(connect_futures, timeout)

    # noinspection PyUnresolvedReferences
    def __connect(self, connector: Connector, channels: Optional[Channels] = None) -> ConnectTask:
        self._logger.debug(f"Connecting {type(connector).__name__} '{connector.name}': {connector.id}")
        if channels is None:
            channels = self.context.filter(lambda c: c.has_connector(connector.id))
            channels.update(self.context.filter(lambda c: c.has_logger(connector.id)).apply(lambda c: c.from_logger()))

        return ConnectTask(connector, channels)

    def __connect_futures(
        self,
        tasks: Dict[Future, ConnectTask],
        timeout: Optional[float] = None,
    ) -> None:
        try:
            for future in futures.as_completed(tasks, timeout=timeout):
                tasks.pop(future)
                self.__connect_callback(future)

        except TimeoutError:
            for future, task in tasks.items():
                self._logger.warning(f"Timed out opening connector '{task.connector.id}' after {timeout} seconds")
                future.cancel()

    def __connect_callback(self, future: Future) -> None:
        try:
            connector = future.result()
            self._logger.info(f"Connected {type(connector).__name__} '{connector.name}': {connector.id}")

        except ConnectorError as e:
            self._logger.warning(f"Failed opening connector '{e.connector.id}': {str(e)}")
            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.exception(e)

    # noinspection PyShadowingBuiltins
    def reconnect(
        self,
        filter: Optional[Callable[[Connector], bool]] = None,
    ) -> None:
        self._reconnect(*self.filter(filter))

    def _reconnect(self, *connectors: Connector) -> None:
        for connector in connectors:
            if not connector.is_enabled():
                self._logger.debug(
                    f"Skipping reconnecting disabled {type(connector).__name__} '{connector.name}': {connector.id}"
                )
                continue

            if not connector._is_connected() and connector._connected:
                # Connection aborted and not yet disconnected properly
                self._disconnect(connector)
                continue

            connect_task = self.__connect(connector)
            connect_future = self._executor.submit(connect_task)
            connect_future.add_done_callback(self.__connect_callback)

    # noinspection PyShadowingBuiltins
    def disconnect(
        self,
        filter: Optional[Callable[[Connector], bool]] = None,
    ) -> None:
        self._disconnect(*self.filter(filter))

    def _disconnect(self, *connectors: Connector) -> None:
        for connector in reversed(connectors):
            if not connector._is_connected():
                self._logger.debug(
                    f"Skipping to disconnect unconnected {type(connector).__name__} '{connector.name}': {connector.id}"
                )
                continue
            self.__disconnect(connector)

    def __disconnect(self, connector: Connector) -> None:
        try:
            self._logger.debug(f"Disconnecting {type(connector).__name__} '{connector.name}': {connector.id}")
            connector.set_channels(ChannelState.DISCONNECTING)
            connector.disconnect()

            self._logger.info(f"Disconnected {type(connector).__name__} '{connector.name}': {connector.id}")

        except Exception as e:
            self._logger.warning(f"Failed closing connector '{connector.id}': {str(e)}")
            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.exception(e)
        finally:
            connector.set_channels(ChannelState.DISCONNECTED)
