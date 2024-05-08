# -*- coding: utf-8 -*-
"""
    loris.data.manager
    ~~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Optional

import pytz as tz
import pandas as pd
import datetime as dt
import logging

from loris import Configurations, Channel
from loris.data.context import DataContext

logger = logging.getLogger(__name__)


class DataManager(DataContext):

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)

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
        logger.info(f"Activating {type(self).__name__}")
        self.components.activate()
        self.connectors.connect()

        self.__activate__()

    def deactivate(self):
        logger.info(f"Deactivating {type(self).__name__}")
        self.components.deactivate()
        self.connectors.close()

        self.__deactivate__()

    # noinspection PyProtectedMember, PyTypeChecker
    def read(self,
             start: Optional[pd.Timestamp, dt.datetime] = None,
             end:   Optional[pd.Timestamp, dt.datetime] = None) -> pd.DataFrame:
        data = []
        for uuid, connector in self.connectors.items():
            time = pd.Timestamp.now(tz=tz.UTC)

            connector_channels = self.filter(lambda c: c.has_reader(uuid)).values()
            if len(connector_channels) == 0:
                continue
            try:
                logger.debug(f"Reading {len(connector_channels)} channels of {type(connector).__name__}: {uuid}")
                connector.read(connector_channels, start, end)
                connector_data = connector_channels.to_frame(unique=True)
                data.append(connector_data)

                def update_reader(channel: Channel) -> None:
                    channel.reader.time = time
                connector_channels.apply(update_reader)

            except Exception as e:
                logger.warning(f"Error reading connector \"{uuid}\": {e}")
                logger.exception(e)
        return pd.concat(data, axis='columns')

    # noinspection PyProtectedMember
    def write(self) -> None:
        for uuid, connector in self.connectors.items():

            def has_update(channel: Channel) -> bool:
                return channel.time > channel.writer.time or pd.isna(channel.writer.time)
            connector_channels = self.filter(lambda c: (c.has_writer(uuid) and has_update(c))).values()
            if len(connector_channels) == 0:
                continue
            try:
                logger.debug(f"Writing {len(connector_channels)} channels with {type(connector).__name__}: {uuid}")
                connector.write(connector_channels)

                def update_writer(channel: Channel) -> None:
                    channel.writer.time = channel.time
                connector_channels.apply(update_writer)

            except Exception as e:
                logger.warning(f"Error writing connector \"{uuid}\": {e}")
                logger.exception(e)
