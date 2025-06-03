# -*- coding: utf-8 -*-
"""
lori.data.replication
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any, Dict, Mapping, Optional

import tzlocal

import pandas as pd
import pytz as tz
from lori import ConfigurationException, Resource, ResourceException, Resources
from lori.connectors import Database
from lori.util import floor_date, parse_freq, slice_range, to_bool, to_timedelta, to_timezone

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class Replicator:
    SECTION: str = "replication"

    _enabled: bool = False

    database: Database

    method: Literal["push", "pull"]
    floor: Optional[str]
    freq: str
    slice: bool
    timezone: tz.BaseTzInfo

    # noinspection PyShadowingNames
    @classmethod
    def build(cls, databases, resource: Resource, **configs) -> Replicator:
        resource_configs = deepcopy(resource.get(cls.SECTION, None))
        if resource_configs is None:
            resource_configs = {"database": None}
        if isinstance(resource_configs, str):
            resource_configs = {"database": resource_configs}
        elif not isinstance(resource_configs, Mapping):
            raise ConfigurationException("Invalid resource replication database: " + str(resource_configs))
        elif "database" not in resource_configs:
            resource_configs["database"] = None

        database = None
        database_id = resource_configs.pop("database")
        if database_id is not None:
            database_path = resource.id.split(".")
            for i in reversed(range(len(database_path))):
                _database_id = ".".join([*database_path[:i], database_id])
                if _database_id in databases.keys():
                    database = databases.get(_database_id, None)
                    break

        configs.update(resource_configs)
        return cls(database, **configs)

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        database: Optional[Database],
        timezone: Optional[tz.BaseTzInfo] = None,
        method: Literal["push", "pull"] = "push",
        floor: Optional[str] = None,
        freq: str = "D",
        slice: bool = True,
        enabled: bool = True,
    ) -> None:
        self._logger = logging.getLogger(self.__module__)
        self._enabled = to_bool(enabled)

        self.database = self._assert_database(database)

        if timezone is None:
            timezone = to_timezone(tzlocal.get_localzone_name())
        self.timezone = timezone

        if method not in ["push", "pull"]:
            raise ConfigurationException(f"Invalid replication method '{method}'")
        self.method = method
        self.floor = parse_freq(floor)
        self.freq = parse_freq(freq)
        self.slice = to_bool(slice)

    @classmethod
    def _assert_database(cls, database):
        if database is None:
            return None
        if not isinstance(database, Database):
            raise ResourceException(database, f"Invalid database: {None if database is None else type(database)}")
        return database

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Replicator) and self._enabled == other._enabled and self._get_args() == other._get_args()
        )

    def __hash__(self) -> int:
        return hash((self.database, self._enabled, *self._get_args()))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.id})"

    def __str__(self) -> str:
        return (
            f"{type(self).__name__}:\n\tid={self.id}\n\t"
            + "\n\t".join(f"{k}={v}" for k, v in self._get_args().items())
            + f"\n\tenabled={self.enabled}"
        )

    @property
    def id(self) -> Optional[str]:
        return self.database.id if self.database is not None else None

    @property
    def key(self) -> str:
        return self.database.key if self.database is not None else None

    # noinspection PyShadowingBuiltins
    def _get_args(self) -> Dict[str, Any]:
        return {
            "method": self.method,
            "floor": self.floor,
            "freq": self.freq,
            "slice": self.slice,
            "timezone": self.timezone,
        }

    @property
    def enabled(self) -> bool:
        return self._enabled and self.database is not None and self.database.is_enabled()

    # noinspection PyProtectedMember
    def replicate(self, resources: Resources, full: bool = False, force: bool = False) -> None:
        if not self.enabled:
            raise ReplicationException(self.database, "Replication disabled")
        if not self.database.is_connected():
            raise ReplicationException(self.database, f"Replication database '{self.database.id}' not connected")
        kwargs = self._get_args()
        kwargs["full"] = full
        kwargs["force"] = force
        method = kwargs.pop("method")

        for logger, logger_resources in resources.groupby(lambda c: c.logger._connector):
            for _, group_resources in logger_resources.groupby(lambda c: c.group):
                try:
                    if method == "push":
                        replicate(logger, self.database, group_resources, **kwargs)
                    elif method == "pull":
                        replicate(self.database, logger, group_resources, **kwargs)

                except ReplicationException as e:
                    self._logger.error(f"Replication failed because: {e}")


class ReplicationException(ResourceException):
    """
    Raise if an error occurred while replicating.

    """


# noinspection PyProtectedMember, PyUnresolvedReferences, PyTypeChecker, PyShadowingBuiltins
def replicate(
    source: Database,
    target: Database,
    resources: Resources,
    timezone: Optional[tz.BaseTzInfo] = None,
    floor: str = None,
    freq: str = "D",
    full: bool = True,
    force: bool = False,
    slice: str = True,
) -> None:
    if source is None or target is None or len(resources) == 0:
        return

    logger = logging.getLogger(Replicator.__module__)
    logger.debug(
        f"Starting to replicate data of resource{'s' if len(resources) > 1 else ''} "
        + ", ".join([f"'{r.id}'" for r in resources])
    )
    target_empty = False

    if timezone is None:
        timezone = to_timezone(tzlocal.get_localzone_name())
    now = pd.Timestamp.now(tz=timezone)
    end = source.read_last_index(resources)
    if end is None:
        end = now
    if floor is not None:
        end = floor_date(end, freq=floor)

    start = target.read_last_index(resources) if not full else None
    if start is None:
        start = source.read_first_index(resources)
        target_empty = True
    else:
        start = floor_date(start, freq=freq) + pd.Timedelta(seconds=1)

    if any(t is None for t in [start, end]) or start >= end:
        logger.debug(
            f"Skip copying values of resource{'s' if len(resources) > 1 else ''} "
            + ", ".join([f"'{r.id}'" for r in resources])
            + " without any new values found"
        )
        return

    if slice and start + to_timedelta(freq) >= end:
        slice = False

    if not target_empty:
        # Validate prior step, before continuing
        prior_end = floor_date(start if start <= now else now, freq=freq)
        prior_start = floor_date(start, timezone=timezone, freq=freq) - to_timedelta(freq) + pd.Timedelta(seconds=1)
        replicate_range(source, target, resources, prior_start, prior_end, force=force)

    if slice:
        for slice_start, slice_end in slice_range(start, end, timezone=timezone, freq=freq):
            replicate_range(source, target, resources, slice_start, slice_end, force=force)
    else:
        replicate_range(source, target, resources, start, end, force=force)

    logger.info(
        f"Replicated {len(data)} values of resource{'s' if len(resources) > 1 else ''} "
        + ", ".join([f"'{r.id}'" for r in resources])
        + f" from {start.strftime('%d.%m.%Y (%H:%M:%S)')}"
        + f" to {end.strftime('%d.%m.%Y (%H:%M:%S)')}"
    )


def replicate_range(
    source: Database,
    target: Database,
    resources: Resources,
    start: pd.Timestamp,
    end: pd.Timestamp,
    force: bool = False,
) -> None:
    logger = logging.getLogger(Replicator.__module__)
    logger.debug(
        f"Start copying data of resource{'s' if len(resources) > 1 else ''} "
        + ", ".join([f"'{r.id}'" for r in resources])
        + f" from {start.strftime('%d.%m.%Y (%H:%M:%S)')}"
        + f" to {end.strftime('%d.%m.%Y (%H:%M:%S)')}"
    )

    source_checksum = source.hash(resources, start, end)
    if source_checksum is None:
        logger.debug(
            f"Skipping time slice without database data for resource{'s' if len(resources) > 1 else ''} "
            + ", ".join([f"'{r.id}'" for r in resources]),
        )
        return

    target_checksum = target.hash(resources, start, end)
    if target_checksum == source_checksum:
        logger.debug(
            f"Skipping time slice without changed data for resource{'s' if len(resources) > 1 else ''} "
            + ", ".join([f"'{r.id}'" for r in resources])
        )
        return

    data = source.read(resources, start=start, end=end)
    if data is None or data.empty:  # not source.exists(resources, start=start, end=end):
        logger.debug(
            f"Skipping time slice without new data for resource{'s' if len(resources) > 1 else ''} ",
            ", ".join([f"'{r.id}'" for r in resources]),
        )
        return

    logger.debug(
        f"Copying {len(data)} values of resource{'s' if len(resources) > 1 else ''} "
        + ", ".join([f"'{r.id}'" for r in resources])
        + f" from {start.strftime('%d.%m.%Y (%H:%M:%S)')}"
        + f" to {end.strftime('%d.%m.%Y (%H:%M:%S)')}"
    )

    target.write(data)
    target_checksum = target.hash(resources, start, end)
    if target_checksum != source_checksum:
        if force:
            target.delete(resources, start, end)
            target.write(data)
            return

        logger.error(
            f"Mismatching for {len(data)} values of resource{'s' if len(resources) > 1 else ''} "
            + ",".join([f"'{r.id}'" for r in resources])
            + f" with checksum '{source_checksum}' against target checksum '{target_checksum}'"
            + f" from {start.strftime('%d.%m.%Y (%H:%M:%S)')}"
            + f" to {end.strftime('%d.%m.%Y (%H:%M:%S)')}"
        )
        raise ReplicationException("Checksum mismatch while synchronizing")
