# -*- coding: utf-8 -*-
"""
lori.data.databases
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from typing import Collection

from lori.connectors import ConnectorContext, ConnectorException, Database
from lori.core import Configurations
from lori.data.channels import Channel, Channels, ChannelState
from lori.data.context import DataContext
from lori.data.replicator import Replicator

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class Databases(ConnectorContext):
    SECTION: str = "databases"

    # noinspection PyUnresolvedReferences
    def __init__(self, context: DataContext, configs: Configurations) -> None:
        super().__init__(context, configs)

    # noinspection PyProtectedMember
    def _load(
        self,
        context: ConnectorContext,
        configs: Configurations,
        configs_file: str = "databases.conf",
    ) -> None:
        super()._load(context, configs, configs_file)

    def extend(self, databases: Collection[Database]) -> None:
        for database in databases:
            if database.is_enabled() and isinstance(database, Database):
                self._add(database)

    def replicate(self, channels: Channels, **kwargs) -> None:
        def build_replicator(channel: Channel) -> Channel:
            channel = channel.from_logger()
            channel.replicator = Replicator.build(self, channel, **kwargs)
            return channel

        # noinspection PyProtectedMember
        def is_replicating(channel: Channel) -> bool:
            return (
                channel.replicator.enabled
                and channel.logger.enabled
                and isinstance(channel.logger._connector, Database)
            )

        replication_channels = channels.apply(build_replicator).filter(is_replicating)
        for database in self.values():
            database_connected = database.is_connected()
            try:
                if not database_connected:
                    self._logger.info(f"Connecting {type(database).__name__} '{database.name}': {database.id}")
                    database.set_channels(ChannelState.CONNECTING)
                    database.connect(replication_channels.filter(lambda c: c.replicator.database.id == database.id))

                    database.set_channels(ChannelState.CONNECTED)
                    self._logger.debug(f"Connected {type(database).__name__} '{database.name}': {database.id}")

                for replicator, replicator_channels in replication_channels.groupby(lambda c: c.replicator):
                    replicator.replicate(replicator_channels)

            except ConnectorException as e:
                self._logger.warning(f"Error opening connector '{e.connector.id}': {str(e)}")
                if self._logger.isEnabledFor(logging.DEBUG):
                    self._logger.exception(e)
            finally:
                if database.is_connected() and not database_connected:
                    database.set_channels(ChannelState.DISCONNECTING)
                    database.disconnect()
                    database.set_channels(ChannelState.DISCONNECTED)
