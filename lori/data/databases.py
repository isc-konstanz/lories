# -*- coding: utf-8 -*-
"""
lori.data.databases
~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from typing import Any, Collection

import tzlocal

import pandas as pd
from lori.connectors import ConnectorContext, Database
from lori.core import Configurations, Configurator, ResourceException
from lori.data.channels import Channel, Channels
from lori.data.context import DataContext
from lori.data.replication import Replicator
from lori.data.retention import Retention, Retentions
from lori.util import floor_date, parse_freq, to_bool, to_timedelta, to_timezone


class Databases(ConnectorContext, Configurator):
    SECTION: str = "databases"

    # noinspection PyProtectedMember, PyUnresolvedReferences
    def __init__(self, context: DataContext, configs: Configurations) -> None:
        super().__init__(context, configs=configs.get_section(Databases.SECTION, defaults={}))
        for database in context.connectors.filter(lambda c: c.is_enabled() and isinstance(c, Database)):
            self._add(database)
        self.load()

    @classmethod
    def _assert_configs(cls, configs: Configurations) -> Configurations:
        if configs is None:
            raise ResourceException(f"Invalid '{cls.__name__}' NoneType configurations")
        return super()._assert_configs(configs)

    def load(self, **kwargs: Any) -> Collection[Database]:
        return self._load(
            self,
            self.configs,
            configs_file=self.configs.name,
            configs_dir=self.configs.dirs.conf.joinpath(self.configs.name.replace(".conf", ".d")),
            includes=Database.INCLUDES,
            **kwargs,
        )

    # noinspection PyUnresolvedReferences
    def replicate(self, channels: Channels, full: bool = False, force: bool = False, **kwargs) -> None:
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
            database_channels = replication_channels.filter(lambda c: c.replicator.database.id == database.id)
            if len(database_channels) == 0:
                continue
            try:
                if not database_connected:
                    self.context.connect(database, channels=database_channels)

                for replicator, replicator_channels in database_channels.groupby(lambda c: c.replicator):
                    replicator.replicate(replicator_channels, full=to_bool(full), force=to_bool(force))

            except ResourceException as e:
                self._logger.warning(f"Error replicating database '{database.id}': {str(e)}")
                if self._logger.isEnabledFor(logging.DEBUG):
                    self._logger.exception(e)
            finally:
                if database.is_connected() and not database_connected:
                    self.context.disconnect(database)

    # noinspection PyProtectedMember
    def rotate(self, channels: Channels, full: bool = False) -> None:
        retentions = Retentions()

        def build_rotation(channel: Channel) -> Channel:
            channel = channel.from_logger()
            channel.rotate = parse_freq(channel.get("rotate", default=None))
            channel.retentions = Retention.build(self.configs, channel)
            retentions.extend(channel.retentions, unique=True)
            return channel

        channels = channels.apply(build_rotation)
        for rotation, rotation_channels in channels.filter(lambda c: c.rotate is not None).groupby(lambda c: c.rotate):
            freq = self.configs.get("freq", default="D")
            timezone = to_timezone(self.configs.get("timezone", default=tzlocal.get_localzone_name()))
            rotate = floor_date(pd.Timestamp.now(tz=timezone) - to_timedelta(rotation), freq=freq)

            for database, database_resources in rotation_channels.groupby(lambda c: c.logger._connector):
                for _, deletion_resources in database_resources.groupby(lambda c: c.group):
                    start = database.read_first_index(deletion_resources)
                    if start is None or start > rotate:
                        self._logger.debug(
                            f"Skip rotating values of resource{'s' if len(deletion_resources) > 1 else ''} "
                            + ", ".join([f"'{r.id}'" for r in deletion_resources])
                            + " without any values found"
                        )
                        continue

                    self._logger.info(
                        f"Deleting values of resource{'s' if len(deletion_resources) > 1 else ''} "
                        + ", ".join([f"'{r.id}'" for r in deletion_resources])
                        + f" up to {rotate.strftime('%d.%m.%Y (%H:%M:%S)')}"
                    )
                    database.delete(deletion_resources, end=rotate)

        retentions.sort()
        for retention in retentions:
            if not retention.enabled:
                self._logger.debug(f"Skipping disabled retention '{retention.keep}'")
                continue
            try:
                # noinspection PyProtectedMember
                def has_retention(channel: Channel) -> bool:
                    return (
                        channel.logger.enabled
                        and isinstance(channel.logger._connector, Database)
                        and retention in channel.retentions
                    )

                retention.aggregate(channels.filter(has_retention), full=to_bool(full))

            except ResourceException as e:
                self._logger.warning(f"Error aggregating '{retention.method}' retaining {retention.keep}: {str(e)}")
