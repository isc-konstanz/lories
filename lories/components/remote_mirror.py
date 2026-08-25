# -*- coding: utf-8 -*-
"""
lories.components.remote_mirror
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A ``remote_mirror`` component mirrors channel data between a remote ``Database``
connector and a local one, either "pull"-ing from the remote into the local
database or "push"-ing the local database's data to the remote one.

"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

import pytz
from lories.components import Component, ComponentError, register_component_type
from lories.core.configs.parameters import Parameter, SelectParameter
from lories.data.channels import Channels
from lories.scheduler import TickScheduler

logger = logging.getLogger(__name__)


@register_component_type("remote_mirror")
class RemoteMirror(Component):
    """
    A ``Component`` that mirrors channel data between a remote and a local ``Database``.

    The mirrored channels are declared like any other component's channels, via the
    inherited ``[<component>.data]`` configuration block; this class does not hardcode
    any channels itself.

    Usage::

        [components.mirror]
        type = "remote_mirror"
        source = "remote_db"    # connector id of the remote database
        mode = "pull"           # copy remote -> local ("push" for the reverse)
        interval = 15           # run every 15 minutes

        [components.mirror.data.channels.power]
        table = "power"
        column = "value"
        type = "float"
        logger = "local_db"     # with no 'target' set, this logger database is the local side

    An explicit ``target = "<connector id>"`` on the component overrides the
    logger-database default, e.g. to mirror into an archive database the
    channels do not log to.

    """

    source = Parameter(
        key="source",
        type=str,
        required=True,
        desc="Connector id of the remote database to mirror from/to",
    )
    target = Parameter(
        key="target",
        type=str,
        required=False,
        desc="Connector id of the local database; defaults to the mirrored channels' logger database",
    )
    mode = SelectParameter(
        ["pull", "push"],
        key="mode",
        default="pull",
        desc="Copy direction: 'pull' copies remote to local, 'push' local to remote",
    )
    full = Parameter(
        key="full",
        type=bool,
        default=True,
        desc="Copy the full history from the source's first record instead of resuming after the target's last",
    )
    force = Parameter(
        key="force",
        type=bool,
        default=False,
        desc="Rewrite time slices even when source and target checksums match",
    )
    slice = Parameter(
        key="slice",
        type=str,
        default="D",
        desc="Chunk size the copy is sliced and checksummed by (pandas freq, e.g. 'D', 'h')",
    )
    freq = Parameter(
        key="freq",
        type=str,
        default="D",
        desc="Frequency the copied range is floored and the prior-step validation window sized by (pandas freq)",
    )
    interval = Parameter(key="interval", type=int, default=60, desc="Mirror schedule interval (minutes)")
    offset = Parameter(key="offset", type=int, default=0, desc="Mirror schedule offset within interval (minutes)")

    source: str
    target: Optional[str]
    mode: str
    full: bool
    force: bool
    slice: str
    freq: str
    interval: int
    offset: int

    _scheduler: Optional[TickScheduler] = None

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

        channels = Channels(list(self.data.values()))
        if len(channels) == 0:
            logger.debug(f"Remote mirror '{self.id}': no channels to mirror")
            return
        source, target = self._resolve_copy_databases(channels)

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

    def _resolve_copy_databases(self, channels: Channels):
        """Resolve the (copy-from, copy-to) databases for the configured ``mode``.

        ``source`` is the remote database and ``target`` the local one; ``pull`` copies
        remote -> local, ``push`` copies local -> remote. An unset ``target`` defaults
        to the logger database shared by all mirrored ``channels``.
        """
        # The mirrored databases are registered on the shared connector context (system level),
        # not on the component's own scoped access, so resolve through `.context`.
        connectors = self.connectors.context
        remote = connectors.get(self.source)
        if remote is None:
            raise ComponentError(self, f"Remote mirror '{self.id}' source database '{self.source}' not available")
        if self.target:
            local = connectors.get(self.target)
            if local is None:
                raise ComponentError(self, f"Remote mirror '{self.id}' target database '{self.target}' not available")
        else:
            local = self._resolve_logger_database(channels)
        if local is remote:
            raise ComponentError(self, f"Remote mirror '{self.id}' source and target must differ: '{self.source}'")
        return (remote, local) if self.mode == "pull" else (local, remote)

    # noinspection PyProtectedMember
    def _resolve_logger_database(self, channels: Channels):
        """Return the single logger database every mirrored channel logs to.

        The fallback target means "mirror into where these channels are logged", so a
        channel without a logger database — or channels logging to different ones —
        is a configuration mistake rather than something to silently paper over.
        """
        databases = set()
        for channel in channels:
            if not channel.logger.is_database():
                raise ComponentError(
                    self,
                    f"Remote mirror '{self.id}' has no target database configured "
                    f"and channel '{channel.id}' does not log to one",
                )
            databases.add(channel.logger._connector)
        if len(databases) > 1:
            ids = ", ".join(sorted(database.id for database in databases))
            raise ComponentError(
                self,
                f"Remote mirror '{self.id}' channels log to different databases ({ids}); configure an explicit target",
            )
        return next(iter(databases))

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
