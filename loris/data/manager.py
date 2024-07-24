# -*- coding: utf-8 -*-
"""
loris.data.manager
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import os
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import Collection, Optional

import pandas as pd
import pytz as tz
from loris import Activator, Channel, Channels, ChannelState, Configurations
from loris.components import ActivatorMeta
from loris.connectors import ConnectorException
from loris.connectors.tasks import ConnectTask, LogTask, ReadTask, WriteTask
from loris.data.context import DataContext
from loris.util import parse_id, parse_name


class DataManagerMeta(ActivatorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        manager = super().__call__(*args, **kwargs)

        manager._DataManager__connect = manager.connect
        manager.connect = manager._do_connect

        manager._DataManager__disconnect = manager.disconnect
        manager.disconnect = manager._do_disconnect

        return manager


class DataManager(DataContext, Activator, metaclass=DataManagerMeta):
    _id: str
    _name: str

    _executor: ThreadPoolExecutor

    def __init__(self, configs: Configurations, name: str, *args, **kwargs) -> None:
        super().__init__(configs=configs, *args, **kwargs)
        self._id = parse_id(name)
        self._name = parse_name(name)
        self._executor = ThreadPoolExecutor(
            thread_name_prefix=self.name, max_workers=max(int((os.cpu_count() or 1) / 2), 1)
        )

    def __enter__(self) -> DataManager:
        self._do_activate()
        return self

    # noinspection PyShadowingBuiltins
    def __exit__(self, type, value, traceback) -> None:
        self._do_deactivate()

    @property
    def uuid(self) -> str:
        return self.id

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    def activate(self) -> None:
        super().activate()
        self._do_connect()
        self._do_activate_members(self._components.values())

    # noinspection PyProtectedMember
    def _do_activate_members(self, activators: Collection[Activator]) -> None:
        for activator in activators:
            if not activator.is_enabled():
                self._logger.debug(
                    f"Skipping activating disabled {type(activator).__name__} '{activator.name}': {activator.uuid}"
                )
                continue

            self._logger.info(f"Activating {type(activator).__name__} '{activator.name}': {activator.uuid}")
            activator._do_activate()

            self._logger.debug(f"Activated {type(activator).__name__} '{activator.name}': {activator.uuid}")

    def connect(self, channels: Optional[Channels]) -> None:
        connect_futures = []
        for uuid, connector in self.connectors.items():
            if not connector.is_enabled():
                self._logger.debug(f"Skipping connecting disabled {type(connector).__name__}: {uuid}")
                continue

            self._logger.info(f"Connecting {type(connector).__name__}: {connector.uuid}")
            connect_channels = channels.filter(lambda c: c.has_connector(uuid) or c.has_logger(uuid))
            connect_futures.append(self._executor.submit(ConnectTask(connector, connect_channels)))

        for connect_future in futures.as_completed(connect_futures):
            try:
                connector = connect_future.result().connector
                self._logger.debug(f"Connected {type(connector).__name__}: {connector.uuid}")

            except ConnectorException as e:
                self._logger.warning(f"Error opening connector '{e.connector.uuid}': {e}")
                self._logger.exception(e)
                e.connector.set_states(ChannelState.UNKNOWN_ERROR)

    @wraps(connect)
    def _do_connect(self, channels: Optional[Channels] = None):
        if channels is None:
            channels = self.values()

        self.__connect(channels)
        self._on_connect(channels)

    def _on_connect(self, channels: Optional[Channels]) -> None:
        pass

    # noinspection PyProtectedMember
    def disconnect(self) -> None:
        for uuid in reversed(list(self.connectors.keys())):
            connector = self.connectors.get(uuid)
            if not connector._is_connected():
                self._logger.debug(f"Skipping disconnecting not connected {type(connector).__name__}: {uuid}")
                continue
            try:
                self._logger.info(f"Disconnecting {type(connector).__name__}: {connector.uuid}")
                connector.set_states(ChannelState.DISCONNECTING)
                connector._do_disconnect()

                self._logger.debug(f"Disconnected {type(connector).__name__}: {connector.uuid}")

            except Exception as e:
                self._logger.warning(f"Error closing connector '{uuid}': {e}")
                self._logger.exception(e)
            finally:
                connector.set_states(ChannelState.DISCONNECTED)

    @wraps(disconnect)
    def _do_disconnect(self):
        self.__disconnect()
        self._on_disconnect()

    def _on_disconnect(self) -> None:
        pass

    def deactivate(self) -> None:
        super().deactivate()
        self._executor.shutdown(wait=True)
        self._do_disconnect()
        self._do_deactivate_members(self._components.values())

    # noinspection PyProtectedMember
    def _do_deactivate_members(self, activators: Collection[Activator]) -> None:
        for activator in reversed(list(activators)):
            if not activator.is_active():
                continue
            try:
                self._logger.info(f"Deactivating {type(activator).__name__} '{activator.name}': {activator.uuid}")
                activator._do_deactivate()

                self._logger.debug(f"Deactivated {type(activator).__name__} '{activator.name}': {activator.uuid}")

            except Exception as e:
                self._logger.warning(f"Error deactivating {type(activator).__name__} '{activator.uuid}': {e}")
                self._logger.exception(e)

    def notify(self, channels: Optional[Channels] = None) -> None:
        pass

    def read(
        self,
        channels: Optional[Channels] = None,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> pd.DataFrame:
        time = pd.Timestamp.now(tz=tz.UTC)
        if channels is None:
            channels = self.values()

        read_tasks = {}
        read_futures = []
        for uuid, connector in self.connectors.items():
            read_channels = channels.filter(lambda c: c.has_connector(uuid))
            if len(read_channels) == 0:
                continue
            read_task = ReadTask(connector, read_channels)
            read_tasks[uuid] = read_task
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
                self._logger.warning(f"Error reading connector '{e.connector.uuid}': {e}")
                self._logger.exception(e)

                def update_state(read_channel: Channel) -> None:
                    read_channel.state = ChannelState.UNKNOWN_ERROR

                read_task = read_tasks[e.connector.uuid]
                read_task.channels.apply(update_state)

        if len(read_data) > 0:
            return pd.concat(read_data, axis="columns")
        return pd.DataFrame()

    def write(self, data: pd.DataFrame, channels: Optional[Channels] = None) -> None:
        time = pd.Timestamp.now(tz=tz.UTC)
        if channels is None:
            channels = self.values()

        write_tasks = {}
        write_futures = []
        for uuid, connector in self.connectors.items():
            write_channels = channels.filter(lambda c: (c.has_connector(uuid) and c.id in data.columns))
            if len(write_channels) == 0:
                continue
            for write_channel in write_channels:
                if len(data.index) > 1:
                    write_channel.set(data.index[0], data.loc[:, write_channel.id])
                elif len(data.index) > 0:
                    timestamp = data.index[-1]
                    write_channel.set(timestamp, data.loc[timestamp, write_channel.id])

            write_task = WriteTask(connector, write_channels)
            write_tasks[uuid] = write_task
            write_futures.append(self._executor.submit(write_task))

        for write_future in futures.as_completed(write_futures):
            try:
                write_task = write_future.result()

                # noinspection PyShadowingNames
                def update_connector(write_channel: Channel) -> None:
                    write_channel.connector.timestamp = time

                write_task.channels.apply(update_connector)

            except ConnectorException as e:
                self._logger.warning(f"Error writing connector '{e.connector.uuid}': {e}")
                self._logger.exception(e)

                # noinspection PyShadowingNames
                def update_state(write_channel: Channel) -> None:
                    write_channel.state = ChannelState.UNKNOWN_ERROR

                write_task = write_tasks[e.connector.uuid]
                write_task.channels.apply(update_state)

    def log(self, channels: Optional[Channels] = None) -> None:
        if channels is None:
            channels = self.values()

        log_tasks = {}
        log_futures = []
        for uuid, connector in self.connectors.items():

            def has_update(channel: Channel) -> bool:
                return pd.isna(channel.logger.timestamp) or channel.logger.timestamp < channel.timestamp

            log_channels = channels.filter(lambda c: (c.has_logger(uuid) and c.is_valid() and has_update(c)))
            if len(log_channels) == 0:
                continue

            log_task = LogTask(connector, log_channels)
            log_tasks[uuid] = log_task
            log_futures.append(self._executor.submit(log_task))

        for write_future in futures.as_completed(log_futures):
            try:
                log_task = write_future.result()

                def update_logger(channel: Channel) -> None:
                    channel.logger.timestamp = channel.timestamp

                log_task.channels.apply(update_logger)

            except ConnectorException as e:
                self._logger.warning(f"Error logging connector '{e.connector.uuid}': {e}")
                self._logger.exception(e)
