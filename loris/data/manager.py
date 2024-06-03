# -*- coding: utf-8 -*-
"""
    loris.data.manager
    ~~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Optional

import os
import pytz as tz
import pandas as pd
import datetime as dt

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from loris import Configurations, Channels, Channel, ChannelState
from loris.connectors import ConnectorException
from loris.connectors.tasks import ConnectTask, ReadTask, WriteTask
from loris.data.context import DataContext


class DataManager(DataContext):

    _executor: ThreadPoolExecutor

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self._executor = ThreadPoolExecutor(thread_name_prefix="DataManager",
                                            max_workers=max(int((os.cpu_count() or 1) / 2), 1))

    def __enter__(self) -> DataManager:
        self.activate()
        return self

    # noinspection PyShadowingBuiltins
    def __exit__(self, type, value, traceback) -> None:
        self.deactivate()

    def __activate__(self) -> None:
        pass

    def __deactivate__(self) -> None:
        pass

    def activate(self) -> None:
        self._logger.info(f"Activating {type(self).__name__}")
        self.connect()

        self.components.activate()
        self.__activate__()

    def connect(self, channels: Optional[Channels] = None) -> None:
        if channels is None:
            channels = self.values()

        connect_futures = []
        for uuid, connector in self.connectors.items():
            connect_channels = channels.filter(lambda c: c.has_connector(uuid) or c.has_logger(uuid))
            connect_futures.append(self._executor.submit(ConnectTask(connector, connect_channels)))

        for connect_future in futures.as_completed(connect_futures):
            try:
                connect_future.result()

            except ConnectorException as e:
                self._logger.warning(f"Error opening connector \"{e.connector.uuid}\": {e}")
                self._logger.exception(e)
                e.connector.set_states(ChannelState.UNKNOWN_ERROR)

    def disconnect(self):
        for uuid, connector in self.connectors.items():
            try:
                self._logger.info(f"Closing {type(connector).__name__}: {uuid}")
                connector.set_states(ChannelState.DISCONNECTING)
                connector.disconnect()

            except Exception as e:
                self._logger.warning(f"Error closing connector \"{uuid}\": {e}")
                self._logger.exception(e)
            finally:
                connector.set_states(ChannelState.DISCONNECTED)

    def deactivate(self):
        self._logger.info(f"Deactivating {type(self).__name__}")
        self._executor.shutdown(wait=True)
        self.disconnect()

        self.components.deactivate()
        self.__deactivate__()

    def notify(self, channels: Optional[Channels] = None) -> None:
        pass

    def read(self, channels: Optional[Channels] = None,
             start: Optional[pd.Timestamp, dt.datetime] = None,
             end:   Optional[pd.Timestamp, dt.datetime] = None) -> pd.DataFrame:

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
                self._logger.warning(f"Error reading connector \"{e.connector.uuid}\": {e}")
                self._logger.exception(e)

                def update_state(read_channel: Channel) -> None:
                    read_channel.state = ChannelState.UNKNOWN_ERROR
                read_task = read_tasks[e.connector.uuid]
                read_task.channels.apply(update_state)

        if len(read_data) > 0:
            return pd.concat(read_data, axis='columns')
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
                self._logger.warning(f"Error writing connector \"{e.connector.uuid}\": {e}")
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
            log_channels = channels.filter(lambda c: (c.has_logger(uuid) and has_update(c)))
            if len(log_channels) == 0:
                continue

            log_task = WriteTask(connector, log_channels)
            log_tasks[uuid] = log_task
            log_futures.append(self._executor.submit(log_task))

        for write_future in futures.as_completed(log_futures):
            try:
                log_task = write_future.result()

                def update_logger(channel: Channel) -> None:
                    channel.logger.timestamp = channel.timestamp
                log_task.channels.apply(update_logger)

            except ConnectorException as e:
                self._logger.warning(f"Error logging connector \"{e.connector.uuid}\": {e}")
                self._logger.exception(e)
