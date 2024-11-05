# -*- coding: utf-8 -*-
"""
lori.data.manager
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import logging
import os
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, Mapping, Optional, Type

import pandas as pd
import pytz as tz
from lori.components.component import Component
from lori.components.context import ComponentContext
from lori.connectors import Connector, ConnectorException
from lori.connectors.context import ConnectorContext
from lori.connectors.tasks import ConnectTask, LogTask, ReadTask, WriteTask
from lori.core import Activator
from lori.core.configs import ConfigurationException, Configurations, Configurator
from lori.core.register import RegistratorContext
from lori.data.channels import Channel, ChannelConnector, Channels, ChannelState
from lori.data.context import DataContext
from lori.util import floor_date, get_variables


# noinspection PyProtectedMember
class DataManager(DataContext, Activator):
    _executor: ThreadPoolExecutor

    _connectors: ConnectorContext
    _components: ComponentContext

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs=configs, *args, **kwargs)
        self._connectors = ConnectorContext(self, configs)
        self._components = ComponentContext(self, configs)
        self._executor = ThreadPoolExecutor(
            thread_name_prefix=self.name, max_workers=max(int((os.cpu_count() or 1) / 2), 1)
        )

    def __contains__(self, item: str | Channel | Connector | Component) -> bool:
        if isinstance(item, str):
            return item in self._channels.keys()
        if isinstance(item, Channel):
            return item in self._channels.values()
        if isinstance(item, Connector) or isinstance(item, Component):
            return item in self._connectors.values() or item in self._components.values()
        return False

    # noinspection PyShadowingBuiltins
    def _new(self, id: str, key: str, type: Type, **configs: Any) -> Channel:
        # noinspection PyShadowingBuiltins
        def build_args(
            registrator_context: RegistratorContext,
            registrator_type: str,
            name: Optional[str] = None,
        ) -> Dict[str, Any]:
            if name is None:
                name = registrator_type
            registrator_section = configs.pop(name, None)
            if registrator_section is None:
                return {registrator_type: None}
            if isinstance(registrator_section, str):
                registrator_section = {registrator_type: registrator_section}
            elif not isinstance(registrator_section, Mapping):
                raise ConfigurationException(f"Invalid channel {name} type: " + str(registrator_section))
            elif registrator_type not in registrator_section:
                return {registrator_type: None}

            registrator_id = registrator_section.pop(registrator_type)
            if "." not in registrator_id:
                _registrator_id = id.replace(key, registrator_id)
                if _registrator_id in registrator_context.keys():
                    registrator_id = _registrator_id
                else:
                    registrator_id = f"{self.id}.{registrator_id}"
            return {registrator_type: registrator_context.get(registrator_id, None), **registrator_section}

        connector = ChannelConnector(**build_args(self._connectors, "connector"))
        logger = ChannelConnector(**build_args(self._connectors, "connector", "logger"))

        return Channel(id=id, key=key, type=type, connector=connector, logger=logger, **configs)

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._load(self, configs)

        self._configure(*get_variables(self._components.values(), include=ComponentContext))

        self._configure(self._connectors)
        self._configure(self._components)

        self._configure(*get_variables(self._components.values(), exclude=ComponentContext))
        self._configure(*get_variables(self._connectors.values(), exclude=Component))

        self._components._sort()
        self._connectors._sort()

    def _configure(self, *configurators: Configurator) -> None:
        for configurator in configurators:
            if not configurator.is_enabled():
                self._logger.debug(
                    f"Skipping configuring disabled {type(configurator).__name__}: " f"{configurator.configs.name}"
                )
                continue
            self._logger.debug(f"Configuring {type(self).__name__}: {configurator.configs.path}")
            configurations = configurator.configs
            configurator.configure(configurations)

            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug(f"Configured {configurator}")

    def activate(self) -> None:
        super().activate()
        self.connect(*self._connectors.values())
        self._activate(*self._components.values())

    def _activate(self, *activators: Activator) -> None:
        for activator in activators:
            if not activator.is_enabled():
                self._logger.debug(
                    f"Skipping activating disabled {type(activator).__name__} '{activator.name}': {activator.id}"
                )
                continue

            self._logger.info(f"Activating {type(activator).__name__} '{activator.name}': {activator.id}")
            activator.activate()

            self._logger.debug(f"Activated {type(activator).__name__} '{activator.name}': {activator.id}")

    def connect(self, *connectors: Connector, channels: Optional[Channels] = None) -> None:
        connect_futures = []
        for connector in connectors:
            if not connector.is_enabled():
                self._logger.debug(f"Skipping connecting disabled {type(connector).__name__}: {connector.id}")
                continue
            if connector._is_connected():
                self._logger.debug(f"Skipping already connected {type(connector).__name__}: {connector.id}")
                continue
            connect_futures.append(self._connect(connector, channels))
        for connect_future in futures.as_completed(connect_futures):
            self._connect_callback(connect_future)
        # for connect_future in connect_futures:
        #     connect_future.add_done_callback(self._connect_callback)

    def _connect(self, connector: Connector, channels: Optional[Channels] = None) -> Future:
        self._logger.info(f"Connecting {type(connector).__name__}: {connector.id}")
        if channels is None:
            channels = self.channels.filter(lambda c: c.has_connector(connector.id))
            channels.update(self.channels.filter(lambda c: c.has_logger(connector.id)).apply(lambda c: c.from_logger()))

        return self._executor.submit(ConnectTask(connector, channels))

    def _connect_callback(self, future: Future) -> None:
        try:
            connector = future.result().connector
            self._logger.debug(f"Connected {type(connector).__name__}: {connector.id}")

        except ConnectorException as e:
            self._logger.warning(f"Error opening connector '{e.connector.id}': {repr(e)}")
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.exception(e)

    def reconnect(self, *connectors: Connector) -> None:
        connect_futures = []
        for connector in connectors:
            if not connector.is_enabled():
                self._logger.debug(f"Skipping reconnecting disabled {type(connector).__name__}: {connector.id}")
                continue
            if not connector._is_connected() and connector._connected:
                # Connection aborted and not yet disconnected properly
                self._disconnect(connector)
                continue
            connect_futures.append(self._connect(connector))
        for connect_future in futures.as_completed(connect_futures):
            self._connect_callback(connect_future)
        # for connect_future in connect_futures:
        #     connect_future.add_done_callback(self._connect_callback)

    def disconnect(self, *connectors: Connector) -> None:
        for connector in reversed(connectors):
            if not connector._is_connected():
                self._logger.debug(f"Skipping disconnecting not connected {type(connector).__name__}: {connector.id}")
                continue
            self._disconnect(connector)

    def _disconnect(self, connector: Connector) -> None:
        try:
            self._logger.info(f"Disconnecting {type(connector).__name__}: {connector.id}")
            connector.set_channels(ChannelState.DISCONNECTING)
            connector.disconnect()

            self._logger.debug(f"Disconnected {type(connector).__name__}: {connector.id}")

        except Exception as e:
            self._logger.warning(f"Error closing connector '{connector.id}': {repr(e)}")
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.exception(e)
        finally:
            connector.set_channels(ChannelState.DISCONNECTED)

    def deactivate(self) -> None:
        super().deactivate()
        self._executor.shutdown(wait=True)
        self._deactivate(*self._components.values())
        self.disconnect(*self._connectors.values())

    def _deactivate(self, *activators: Activator) -> None:
        for activator in reversed(list(activators)):
            if not activator.is_active():
                continue
            try:
                self._logger.info(f"Deactivating {type(activator).__name__} '{activator.name}': {activator.id}")
                activator.deactivate()

                self._logger.debug(f"Deactivated {type(activator).__name__} '{activator.name}': {activator.id}")

            except Exception as e:
                self._logger.warning(f"Error deactivating {type(activator).__name__} '{activator.id}': {repr(e)}")
                if self._logger.isEnabledFor(logging.DEBUG):
                    self._logger.exception(e)

    @property
    def components(self) -> ComponentContext:
        return self._components

    @property
    def connectors(self) -> ConnectorContext:
        return self._connectors

    def notify(self, channels: Optional[Channels] = None) -> None:
        pass

    # noinspection PyShadowingBuiltins
    def read(
        self,
        channels: Optional[Channels] = None,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame:
        time = pd.Timestamp.now(tz=tz.UTC)
        if channels is None:
            channels = self.channels

        read_tasks = {}
        read_futures = []
        for id, connector in self.connectors.items():
            if not connector._is_connected():
                continue

            read_channels = channels.filter(lambda c: c.has_connector(id))
            if len(read_channels) == 0:
                continue
            read_task = ReadTask(connector, read_channels)
            read_tasks[id] = read_task
            read_futures.append(self._executor.submit(read_task, start=start, end=end))

        read_data = []
        for read_future in futures.as_completed(read_futures):
            try:
                read_channels = read_future.result().channels
                read_data.append(channels.to_frame(unique=True))

                def update_connector(read_channel: Channel) -> None:
                    read_channel.connector.timestamp = time

                read_channels.apply(update_connector)

            except ConnectorException as e:
                self._logger.warning(f"Error reading connector '{e.connector.id}': {repr(e)}")
                if self._logger.isEnabledFor(logging.DEBUG):
                    self._logger.exception(e)

                def update_state(channel: Channel) -> Channel:
                    channel.state = ChannelState.UNKNOWN_ERROR
                    return channel

                read_task = read_tasks[e.connector.id]
                read_task.channels.apply(update_state)

        if len(read_data) > 0:
            return pd.concat(read_data, axis="columns")
        return pd.DataFrame()

    # noinspection PyShadowingBuiltins
    def write(self, data: pd.DataFrame, channels: Optional[Channels] = None) -> None:
        time = pd.Timestamp.now(tz=tz.UTC)
        if channels is None:
            channels = self.channels

        write_tasks = {}
        write_futures = []
        for id, connector in self.connectors.items():
            if not connector._is_connected():
                continue

            write_channels = channels.filter(lambda c: (c.has_connector(id) and c.key in data.columns))
            if len(write_channels) == 0:
                continue
            for write_channel in write_channels:
                if len(data.index) > 1:
                    write_channel.set(data.index[0], data.loc[:, write_channel.key])
                elif len(data.index) > 0:
                    timestamp = data.index[-1]
                    write_channel.set(timestamp, data.loc[timestamp, write_channel.key])

            write_task = WriteTask(connector, write_channels)
            write_tasks[id] = write_task
            write_futures.append(self._executor.submit(write_task))

        for write_future in futures.as_completed(write_futures):
            try:
                write_task = write_future.result()

                # noinspection PyShadowingNames
                def update_connector(channel: Channel) -> Channel:
                    channel.connector.timestamp = time
                    return channel

                write_task.channels.apply(update_connector)

            except ConnectorException as e:
                self._logger.warning(f"Error writing connector '{e.connector.id}': {repr(e)}")
                if self._logger.isEnabledFor(logging.DEBUG):
                    self._logger.exception(e)

                # noinspection PyShadowingNames
                def update_state(channel: Channel) -> Channel:
                    channel.state = ChannelState.UNKNOWN_ERROR
                    return channel

                write_task = write_tasks[e.connector.id]
                write_task.channels.apply(update_state)

    # noinspection PyShadowingBuiltins
    def log(self, channels: Optional[Channels] = None, force: bool = False) -> None:
        if channels is None:
            channels = self.channels

        log_tasks = {}
        log_futures = []
        for id, connector in self.connectors.items():
            if not connector._is_connected():
                continue

            def has_update(channel: Channel) -> bool:
                if force:
                    return True
                if channel.freq is None:
                    return pd.isna(channel.logger.timestamp) or channel.timestamp > channel.logger.timestamp
                if pd.isna(channel.logger.timestamp):
                    logger_timestamp = floor_date(channel.timestamp, freq=channel.freq)
                    if logger_timestamp == channel.timestamp:
                        logger_timestamp -= channel.timedelta
                    channel.logger.timestamp = logger_timestamp

                return channel.timestamp >= channel.logger.timestamp + channel.timedelta

            log_channels = channels.filter(lambda c: (c.has_logger(id) and c.is_valid() and has_update(c)))
            if len(log_channels) == 0:
                continue

            log_task = LogTask(connector, log_channels)
            log_tasks[id] = log_task
            log_futures.append(self._executor.submit(log_task))

        for write_future in futures.as_completed(log_futures):
            try:
                log_task = write_future.result()

                def update_logger(channel: Channel) -> Channel:
                    channel.logger.timestamp = channel.timestamp
                    return channel

                log_task.channels.apply(update_logger)

            except ConnectorException as e:
                self._logger.warning(f"Error logging connector '{e.connector.id}': {repr(e)}")
                if self._logger.isEnabledFor(logging.DEBUG):
                    self._logger.exception(e)
