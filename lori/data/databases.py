# -*- coding: utf-8 -*-
"""
lori.data.backup
~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from typing import Optional

import tzlocal

import pandas as pd
import pytz as tz
from lori import Channels, Resources
from lori.connectors import ConnectorContext, Database, DatabaseException
from lori.core import Configurations
from lori.data.context import DataContext
from lori.util import floor_date, parse_freq, slice_range, to_timedelta, to_timezone

logger = logging.getLogger(__name__)


class Databases(ConnectorContext):
    SECTION: str = "databases"

    def __init__(self, context: DataContext) -> None:
        super().__init__(context, context.configs)

    # noinspection PyProtectedMember
    def _load(
        self,
        context: ConnectorContext,
        configs: Configurations,
        configs_file: str = "databases.conf",
    ) -> None:
        super()._load(context, configs, configs_file)

    # noinspection PyShadowingBuiltins, PyTypeChecker
    def synchronize(
        self,
        source: Database,
        channels: Optional[Channels] = None,
        timezone: Optional[tz.BaseTzInfo] = None,
        freq: str = "D",
        full: bool = False,
        slice: bool = True,
    ) -> None:
        self._logger.info(f"Starting to synchronize data of {len(channels)} channel{'s' if len(channels) > 0 else ''}")
        if channels is None:
            channels = source.channels.apply(lambda c: c.from_logger())
        if timezone is None:
            timezone = to_timezone(tzlocal.get_localzone_name())
        freq = parse_freq(freq)
        now = pd.Timestamp.now(tz=timezone)

        for target in self.values():
            for _, channels in channels.groupby("group"):
                target_empty = False

                end = source.read_last_index(channels)
                if end is None:
                    end = now

                start = target.read_last_index(channels) if not full else None
                if start is None:
                    start = source.read_first_index(channels)
                    target_empty = True
                else:
                    start += pd.Timedelta(seconds=1)

                if (not any(t is None for t in [start, end]) and start >= end) or all(t is None for t in [start, end]):
                    self._logger.debug(
                        f"Skip copying values of channel{'s' if len(channels) > 1 else ''} ",
                        ", ".join([f"'{r.id}'" for r in channels]),
                        " without any new values found",
                    )
                    continue

                if slice and end - start <= to_timedelta(freq):
                    slice = False

                if not target_empty:
                    if start > now:
                        start = floor_date(now, freq=freq)

                    prior = floor_date(start, timezone=timezone, freq=freq) - to_timedelta(freq)
                    self._synchronize(source, target, channels, prior, start)

                if slice:
                    # Validate prior step, before continuing
                    for slice_start, slice_end in slice_range(start, end, timezone=timezone, freq=freq):
                        self._synchronize(source, target, channels, slice_start, slice_end)
                else:
                    self._synchronize(source, target, channels, start, end)

    def _synchronize(
        self,
        source: Database,
        target: Database,
        resources: Resources,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        self._logger.debug(
            f"Start copying data of channel{'s' if len(resources) > 1 else ''} ",
            ", ".join([f"'{r.id}'" for r in resources]),
            f" from {start.strftime('%d.%m.%Y (%H:%M:%S)')}",
            f" to {end.strftime('%d.%m.%Y (%H:%M:%S)')}",
        )

        source_checksum = source.hash(resources, start, end)
        if source_checksum is None:
            self._logger.debug(
                f"Skipping time slice without source data for channel{'s' if len(resources) > 1 else ''} ",
                ", ".join([f"'{r.id}'" for r in resources]),
            )
            return

        target_checksum = target.hash(resources, start, end)
        if target_checksum == source_checksum:
            self._logger.debug(
                f"Skipping time slice without changed data for channel{'s' if len(resources) > 1 else ''} ",
                ", ".join([f"'{r.id}'" for r in resources]),
            )
            return

        data = source.read(resources, start=start, end=end)
        if data is None or data.empty:
            self._logger.debug(
                f"Skipping time slice without new data for channel{'s' if len(resources) > 1 else ''} ",
                ", ".join([f"'{r.id}'" for r in resources]),
            )
            return

        self._logger.info(
            f"Copying {len(data)} values of channel{'s' if len(resources) > 1 else ''} ",
            ", ".join([f"'{r.id}'" for r in resources]),
            f" from {start.strftime('%d.%m.%Y (%H:%M:%S)')}",
            f" to {end.strftime('%d.%m.%Y (%H:%M:%S)')}",
        )

        target.write(data)
        target_checksum = target.hash(resources, start, end)
        if target_checksum != source_checksum:
            self._logger.error(
                f"Mismatching for {len(data)} values of channel{'s' if len(resources) > 1 else ''} ",
                ",".join([f"'{r.id}'" for r in resources]),
                f" with checksum '{source_checksum}' against target checksum '{target_checksum}'",
            )
            raise DatabaseException("Checksum mismatch while synchronizing")
