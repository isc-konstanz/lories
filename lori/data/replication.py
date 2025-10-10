# -*- coding: utf-8 -*-
"""
lori.data.replication
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any, Dict, Iterator, List, Mapping, Optional

import tzlocal

import pandas as pd
import pytz as tz
from lori import ConfigurationError, Resource, ResourceError, Resources
from lori.connectors import Database
from lori.util import floor_date, parse_freq, slice_range, to_bool, to_timedelta, to_timezone

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


class Replication:
    TYPE: str = "replication"

    _enabled: bool = False

    database: Database

    method: Literal["push", "pull"]
    slice: str = "D"
    freq: str = "D"
    timezone: tz.BaseTzInfo

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        database: Optional[Database],
        timezone: Optional[tz.BaseTzInfo] = None,
        method: Literal["push", "pull"] = "push",
        slice: str = "D",
        freq: str = "D",
        enabled: bool = True,
    ) -> None:
        self._logger = logging.getLogger(self.__module__)
        self._enabled = to_bool(enabled)

        self.database = self._assert_database(database)

        if timezone is None:
            timezone = to_timezone(tzlocal.get_localzone_name())
        self.timezone = timezone

        if method not in ["push", "pull"]:
            raise ConfigurationError(f"Invalid replication method '{method}'")
        self.method = method
        self.freq = parse_freq(freq)
        self.slice = parse_freq(slice)

    @classmethod
    def _assert_database(cls, database):
        if database is None:
            return None
        if not isinstance(database, Database):
            raise ResourceError(database, f"Invalid database: {None if database is None else type(database)}")
        return database

    # noinspection PyShadowingBuiltins
    def _is_mapped(
        self,
        database: Optional[str],
        timezone: Optional[tz.BaseTzInfo] = None,
        method: Literal["push", "pull"] = "push",
        slice: str = "D",
        freq: str = "D",
        enabled: bool = True,
    ) -> bool:
        return (
            self.database.key == database
            and self.timezone == to_timezone(timezone)
            and self.method == method
            and self.freq == parse_freq(freq)
            and self.slice == parse_freq(slice)
            and self._enabled == to_bool(enabled)
        )

    def _is_equal(self, other: Replication) -> bool:
        return (
            self._enabled == other._enabled
            and self.database.id == other.database.id
            and self._get_args() == other._get_args()
        )

    def __eq__(self, other: Any) -> bool:
        return self._is_equal(other)

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
            "slice": self.slice,
            "freq": self.freq,
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


class ReplicationException(ResourceError):
    """
    Raise if an error occurred while replicating.

    """


class Replications(Sequence[Replication]):
    _replications: List[Replication]

    # noinspection PyProtectedMember, PyShadowingNames
    def build(self, databases, resource: Resource, **configs) -> Optional[Replication]:
        resource_configs = resource.get(Replication.TYPE, default=None)
        if resource_configs is None or "database" not in resource_configs:
            return None
        if isinstance(resource_configs, str):
            resource_configs = {"database": resource_configs}
        elif not isinstance(resource_configs, Mapping):
            raise ConfigurationError("Invalid resource replication database: " + str(resource_configs))

        replication = None
        for _replication in self:
            if _replication._is_mapped(**resource_configs):
                replication = _replication
                break
        if replication is None:
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
            replication = Replication(database, **configs)

            self._replications.append(replication)
        return replication

    def __init__(self, resources=()) -> None:
        self._logger = logging.getLogger(type(self).__module__)
        self._replications = [*resources]

    # noinspection PyProtectedMember
    def __contains__(self, _object: Mapping[str, Any] | Replication) -> bool:
        if isinstance(_object, Replication):
            return _object in self._replications
        if isinstance(_object, Mapping):
            return any(r._is_mapped(*_object) for r in self._replications)
        return False

    def __getitem__(self, _index: int):
        return self._replications[_index]

    def __iter__(self) -> Iterator[Replication]:
        return iter(self._replications)

    def __len__(self) -> int:
        return len(self._replications)

    def __add__(self, _object):
        return type(self)([*self, *_object])

    def append(self, replication: Replication) -> None:
        self._replications.append(replication)


# noinspection PyProtectedMember, PyUnresolvedReferences, PyTypeChecker, PyShadowingBuiltins
def replicate(
    source: Database,
    target: Database,
    resources: Resources,
    timezone: Optional[tz.BaseTzInfo] = None,
    slice: str = "D",
    freq: str = "D",
    full: bool = True,
    force: bool = False,
) -> None:
    if source is None or target is None or len(resources) == 0:
        return

    logger = logging.getLogger(Replication.__module__)
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
    if not full:
        end = floor_date(end, freq=freq)

    start = target.read_last_index(resources) if not full else None
    if start is None:
        start = source.read_first_index(resources)
        target_empty = True

    if any(t is None for t in [start, end]) or start >= end:
        logger.debug(
            f"Skip copying values of resource{'s' if len(resources) > 1 else ''} "
            + ", ".join([f"'{r.id}'" for r in resources])
            + " without any values found"
        )
        return

    if not target_empty:
        # Validate prior step, before continuing
        prior_end = floor_date(start if start <= now else now, timezone=timezone, freq=freq)
        prior_start = prior_end - to_timedelta(freq) + pd.Timedelta(seconds=1)
        replicate_range(source, target, resources, prior_start, prior_end, force=force)

    if start + to_timedelta(slice) < end:
        for slice_start, slice_end in slice_range(start, end, timezone=timezone, freq=slice):
            replicate_range(source, target, resources, slice_start, slice_end, force=force)
    else:
        replicate_range(source, target, resources, start, end, force=force)


def replicate_range(
    source: Database,
    target: Database,
    resources: Resources,
    start: pd.Timestamp,
    end: pd.Timestamp,
    force: bool = False,
) -> None:
    logger = logging.getLogger(Replication.__module__)
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

    logger.info(
        f"Replicated {len(data)} values of resource{'s' if len(resources) > 1 else ''} "
        + ", ".join([f"'{r.id}'" for r in resources])
        + f" from {start.strftime('%d.%m.%Y (%H:%M:%S)')}"
        + f" to {end.strftime('%d.%m.%Y (%H:%M:%S)')}"
    )
