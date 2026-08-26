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
import threading
from datetime import timedelta
from typing import Optional, Tuple

import pandas as pd
import pytz
from lories.components import Component, ComponentError, register_component_type
from lories.core import Configurations
from lories.core.configs.parameters import DurationParameter, Parameter, SelectParameter
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
        window = "forecast"     # copy only records from now on (control window; default "all")
        horizon = "2D"          # optional forward bound of the forecast window
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
    window = SelectParameter(
        ["all", "historical", "forecast"],
        key="window",
        default="all",
        desc="Copied range: 'all' mirrors the full source history, 'historical' only records up to now, "
        "'forecast' only records from now on (the control window; ignores 'full')",
    )
    horizon = DurationParameter(
        key="horizon",
        default=None,
        required=False,
        desc="Forward extent of the 'forecast' window (e.g. '2D'); unset copies up to the source's last record",
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
        default=None,
        required=False,
        desc="Rewrite time slices whose source and target checksums mismatch after copying; defaults to true "
        "for the 'forecast' window (an exact mirror, so source-side deletions propagate) and false otherwise",
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
    window: str
    horizon: Optional[pd.Timedelta]
    full: bool
    force: Optional[bool]
    slice: str
    freq: str
    interval: int
    offset: int

    _scheduler: Optional[TickScheduler] = None
    _copy_lock: Optional[threading.Lock] = None

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        # One lock per component instance, surviving activate/deactivate cycles: it keeps a copy
        # still in flight after a timed-out scheduler stop from overlapping a re-activated one
        # (TickScheduler itself never overlaps runs within one instance).
        if self._copy_lock is None:
            self._copy_lock = threading.Lock()

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
        A copy still in flight (e.g. across a deactivate/activate cycle whose scheduler stop
        timed out) makes the run skip instead of overlapping it.
        """
        from lories.data.replication import replicate

        if not self._copy_lock.acquire(blocking=False):
            logger.warning(f"Remote mirror '{self.id}': previous copy still running; skipping this run")
            return
        try:
            channels = Channels(list(self.data.values()))
            if len(channels) == 0:
                logger.debug(f"Remote mirror '{self.id}': no channels to mirror")
                return
            source, target = self._resolve_copy_databases(channels)
            start, end = self._resolve_copy_window()

            # Connect the source with the declared channels first: enough to build the table and
            # discover the live surrogate groups, which the enumerated resource-set is built from.
            source.connect(channels)
            try:
                resources = self._enumerate_resources(source, channels, start, end)
                target.connect(resources)
                try:
                    replicate(
                        source,
                        target,
                        resources,
                        timezone=pytz.UTC,
                        slice=self.slice,
                        freq=self.freq,
                        # An explicit window start replaces the resume/full range derivation, and the
                        # forecast end must never be floored to a complete period, so 'full' only
                        # steers the windowless modes.
                        full=self.full if self.window != "forecast" else True,
                        force=self.force if self.force is not None else self.window == "forecast",
                        start=start,
                        end=end,
                    )
                finally:
                    target.disconnect()
            finally:
                source.disconnect()
        finally:
            self._copy_lock.release()

    def _resolve_copy_window(self) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        """Resolve the configured ``window`` to explicit copy bounds (``None`` = unbounded.)

        ``all`` mirrors everything, ``historical`` only records up to now, and ``forecast`` only
        records from now on — the control window: future values written to the copy source (e.g.
        an irrigation schedule) reach the target promptly, bounded by ``horizon`` if configured.
        """
        if self.window == "all":
            return None, None
        now = pd.Timestamp.now(tz=pytz.UTC)
        if self.window == "historical":
            return None, now
        return now, (now + self.horizon) if self.horizon is not None else None

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

    def _enumerate_resources(
        self,
        source,
        channels: Channels,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
    ) -> Channels:
        """Duplicate each mirrored channel once per surrogate group present on ``source``.

        Surrogate-keyed tables (e.g. an append-per-run ``timestamp_creation``) copy one group
        per resource, and the group values are not known ahead of time, so they are discovered
        live (``source`` must already be connected). Discovery is per-table — each table's groups
        apply only to that table's channels — so a channel-set spanning tables with different
        surrogate attributes stays correct. Tables without surrogate keys (``read_groups`` returns
        ``None``) mirror channels as declared; surrogate-keyed tables without any group ``[]`` have
        nothing addressable to copy and drop out of the run entirely — passing their channels along
        bare would fail every read over the missing surrogate attribute.
        ``start``/``end`` bound the discovery to groups with records in the copy window, keeping
        the enumeration flat for windowed mirrors of append-per-run tables.
        """
        resources = []
        for _table, table_channels in self._group_by_table(channels).items():
            table_channels = Channels(table_channels)
            groups = source.read_groups(table_channels, start=start, end=end)
            if groups is None:
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
