# -*- coding: utf-8 -*-
"""
lories.components.remote_mirror
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A ``remote_mirror`` component mirrors channel data between a remote ``Database``
connector and a local one, either "pull"-ing from the remote into the local
database or "push"-ing the local database's data to the remote one.

This module only declares the component's registration, its full config
surface and the lifecycle stubs; the copy engine and its scheduling are
implemented by later units.

"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

import pytz
from lories.components import Component, ComponentError, register_component_type
from lories.core import ConfigurationError
from lories.data.channels import Channels
from lories.scheduler import TickScheduler
from lories.typing import Configurations

logger = logging.getLogger(__name__)


@register_component_type("remote_mirror")
class RemoteMirror(Component):
    """
    A ``Component`` that mirrors channel data between a remote and a local ``Database``.

    The mirrored channels are declared like any other component's channels, via the
    inherited ``[<component>.data]`` configuration block; this class does not hardcode
    any channels itself.

    """

    source: str
    target: Optional[str] = None
    mode: str = "pull"
    full: bool = True
    force: bool = False
    slice: str = "D"
    freq: str = "D"
    interval: int = 60
    offset: int = 0

    _scheduler: Optional[TickScheduler] = None

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self.source = configs.get("source")
        if not self.source:
            raise ConfigurationError(f"Missing 'source' database id for remote mirror '{self.id}'")

        # A later unit may default an unset target to the channels' logger connector.
        self.target = configs.get("target", default=RemoteMirror.target)

        self.mode = configs.get("mode", default=RemoteMirror.mode)
        if self.mode not in ("pull", "push"):
            raise ConfigurationError(f"Invalid remote mirror mode '{self.mode}' for '{self.id}'")

        self.full = configs.get_bool("full", default=RemoteMirror.full)
        self.force = configs.get_bool("force", default=RemoteMirror.force)
        self.slice = configs.get("slice", default=RemoteMirror.slice)
        self.freq = configs.get("freq", default=RemoteMirror.freq)
        self.interval = configs.get_int("interval", default=RemoteMirror.interval)
        self.offset = configs.get_int("offset", default=RemoteMirror.offset)

    def activate(self) -> None:
        super().activate()
        self._scheduler = TickScheduler(
            self._mirror_once,
            interval=timedelta(minutes=self.interval),
            offset=timedelta(minutes=self.offset),
            name=self.id,
        )
        self._scheduler.start()
        logger.debug(f"Activated remote mirror '{self.id}' (every {self.interval}min, offset {self.offset}min)")

    def deactivate(self) -> None:
        # Stop the cadence before the connectors are torn down so a copy is not left running
        # against a half-disconnected database. Best-effort, bounded by the scheduler's join
        # timeout: a copy still in flight past it is a daemon thread that exits on its own.
        if self._scheduler is not None:
            self._scheduler.stop()
            self._scheduler = None
        super().deactivate()
        logger.debug(f"Deactivated remote mirror '{self.id}'")

    def _mirror_once(self) -> None:
        """Run a single copy of the mirrored channel-set between the two databases.

        Discovers the surrogate groups live, enumerates one resource per (channel, group),
        connects both databases, delegates the copy to the replication engine, and disconnects.
        Idempotent per run: unchanged slices are skipped by the engine's per-slice checksums.
        """
        from lories.data.replication import replicate

        source, target = self._resolve_copy_databases()
        channels = Channels(list(self.data.values()))
        if len(channels) == 0:
            logger.debug(f"Remote mirror '{self.id}': no channels to mirror")
            return

        # Connect the source with the declared channels first: enough to build the table and
        # discover the live surrogate groups, which the enumerated resource-set is built from.
        source.connect(channels)
        try:
            resources = self._enumerate_resources(source, channels)
            target.connect(resources)
            try:
                replicate(
                    source,
                    target,
                    resources,
                    timezone=pytz.UTC,
                    slice=self.slice,
                    freq=self.freq,
                    full=self.full,
                    force=self.force,
                )
            finally:
                target.disconnect()
        finally:
            source.disconnect()

    def _resolve_copy_databases(self):
        """Resolve the (copy-from, copy-to) databases for the configured ``mode``.

        ``source`` is the remote database and ``target`` the local one; ``pull`` copies
        remote -> local, ``push`` copies local -> remote.
        """
        # The mirrored databases are registered on the shared connector context (system level),
        # not on the component's own scoped access, so resolve through `.context`.
        connectors = self.connectors.context
        remote = connectors.get(self.source)
        if remote is None:
            raise ComponentError(self, f"Remote mirror '{self.id}' source database '{self.source}' not available")
        if not self.target:
            raise ComponentError(self, f"Remote mirror '{self.id}' requires a configured target database")
        local = connectors.get(self.target)
        if local is None:
            raise ComponentError(self, f"Remote mirror '{self.id}' target database '{self.target}' not available")
        if self.source == self.target:
            raise ComponentError(self, f"Remote mirror '{self.id}' source and target must differ: '{self.source}'")
        return (remote, local) if self.mode == "pull" else (local, remote)

    def _enumerate_resources(self, source, channels: Channels) -> Channels:
        """Duplicate each mirrored channel once per surrogate group present on ``source``.

        Surrogate-keyed tables (e.g. an append-per-run ``timestamp_creation``) copy one group
        per resource, and the group values are not known ahead of time, so they are discovered
        live (``source`` must already be connected). Discovery is per-table — each table's groups
        apply only to that table's channels — so a channel-set spanning tables with different
        surrogate attributes stays correct. Tables without surrogate keys mirror channels as declared.
        """
        resources = []
        for _table, table_channels in self._group_by_table(channels).items():
            table_channels = Channels(table_channels)
            groups = source.read_groups(table_channels)
            if len(groups) == 0:
                resources.extend(table_channels)
                continue
            for channel in table_channels:
                for index, group in enumerate(groups):
                    # A per-group index keeps the resource id unique and valid regardless of the
                    # surrogate value's type (e.g. a timestamp). Default the duplicate to the
                    # channel's own data context (a TaskContext); the ComponentContext is not one.
                    resources.append(channel.duplicate(id=f"{channel.id}.{index}", **group))
        return Channels(resources)

    @staticmethod
    def _group_by_table(channels: Channels) -> dict:
        grouped: dict = {}
        for channel in channels:
            grouped.setdefault(channel.get("table", default=channel.group), []).append(channel)
        return grouped
