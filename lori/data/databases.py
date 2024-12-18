# -*- coding: utf-8 -*-
"""
lori.data.backup
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Optional

import tzlocal

import pandas as pd
import pytz as tz
from lori.connectors import Connector, ConnectorContext, Database, DatabaseException
from lori.core import Configurations, Resources
from lori.data.channels import ChannelConnector, Channels
from lori.data.context import DataContext
from lori.util import floor_date, parse_freq, slice_range, to_timedelta, to_timezone


class Databases(ConnectorContext):
    SECTION: str = "databases"

    # noinspection PyProtectedMember
    @staticmethod
    def is_database(connector: Connector | ChannelConnector) -> bool:
        if isinstance(connector, ChannelConnector):
            if not connector.enabled:
                return False
            connector = connector._connector
        return connector.is_enabled() and isinstance(connector, Database)

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

    # noinspection PyShadowingBuiltins, PyUnresolvedReferences, PyProtectedMember, PyTypeChecker
    def replicate(
        self,
        database: Database,
        channels: Optional[Channels],
        timezone: Optional[tz.BaseTzInfo] = None,
        freq: str = "D",
        full: bool = False,
        slice: bool = True,
    ) -> None:
        self._logger.info(f"Starting to replicate data of {len(channels)} channel{'s' if len(channels) > 0 else ''}")
        if timezone is None:
            timezone = to_timezone(tzlocal.get_localzone_name())
        freq = parse_freq(freq)
        now = pd.Timestamp.now(tz=timezone)

        for logger, logger_channels in channels.groupby(lambda c: c.logger._connector):
            if logger is None or len(logger_channels) == 0:
                continue
            logger_empty = False

            end = database.read_last_index(logger_channels)
            if end is None:
                end = now

            start = logger.read_last_index(logger_channels) if not full else None
            if start is None:
                start = database.read_first_index(logger_channels)
                logger_empty = True
            else:
                start += pd.Timedelta(seconds=1)

            if (not any(t is None for t in [start, end]) and start >= end) or all(t is None for t in [start, end]):
                self._logger.debug(
                    f"Skip copying values of channel{'s' if len(logger_channels) > 1 else ''} "
                    + ", ".join([f"'{r.id}'" for r in logger_channels])
                    + " without any new values found"
                )
                continue

            if slice and end - start <= to_timedelta(freq):
                slice = False

            if not logger_empty:
                if start > now:
                    start = floor_date(now, freq=freq)

                prior = floor_date(start, timezone=timezone, freq=freq) - to_timedelta(freq)
                self._replicate(database, logger, logger_channels, prior, start)

            if slice:
                # Validate prior step, before continuing
                for slice_start, slice_end in slice_range(start, end, timezone=timezone, freq=freq):
                    self._replicate(database, logger, logger_channels, slice_start, slice_end)
            else:
                self._replicate(database, logger, logger_channels, start, end)

    def _replicate(
        self,
        database: Database,
        logger: Database,
        resources: Resources,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        self._logger.debug(
            f"Start copying data of channel{'s' if len(resources) > 1 else ''} "
            + ", ".join([f"'{r.id}'" for r in resources])
            + f" from {start.strftime('%d.%m.%Y (%H:%M:%S)')}"
            + f" to {end.strftime('%d.%m.%Y (%H:%M:%S)')}"
        )

        database_checksum = database.hash(resources, start, end)
        if database_checksum is None:
            self._logger.debug(
                f"Skipping time slice without database data for channel{'s' if len(resources) > 1 else ''} "
                + ", ".join([f"'{r.id}'" for r in resources]),
            )
            return

        logger_checksum = logger.hash(resources, start, end)
        if logger_checksum == database_checksum:
            self._logger.debug(
                f"Skipping time slice without changed data for channel{'s' if len(resources) > 1 else ''} "
                + ", ".join([f"'{r.id}'" for r in resources])
            )
            return

        data = database.read(resources, start=start, end=end)
        if data is None or data.empty:  # not database.exists(resources, start=start, end=end):
            self._logger.debug(
                f"Skipping time slice without new data for channel{'s' if len(resources) > 1 else ''} ",
                ", ".join([f"'{r.id}'" for r in resources]),
            )
            return

        self._logger.info(
            f"Copying {len(data)} values of channel{'s' if len(resources) > 1 else ''} "
            + ", ".join([f"'{r.id}'" for r in resources])
            + f" from {start.strftime('%d.%m.%Y (%H:%M:%S)')}"
            + f" to {end.strftime('%d.%m.%Y (%H:%M:%S)')}"
        )

        logger.write(data)
        logger_checksum = logger.hash(resources, start, end)
        if logger_checksum != database_checksum:
            self._logger.error(
                f"Mismatching for {len(data)} values of channel{'s' if len(resources) > 1 else ''} "
                + ",".join([f"'{r.id}'" for r in resources])
                + f" with checksum '{database_checksum}' against target checksum '{logger_checksum}'"
            )
            raise DatabaseException(logger, "Checksum mismatch while synchronizing")
