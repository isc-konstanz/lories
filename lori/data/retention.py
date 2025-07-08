# -*- coding: utf-8 -*-
"""
lori.data.retention
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, Iterator, MutableSequence, Optional

import tzlocal

import pandas as pd
import pytz as tz
from lori import ConfigurationException, Configurations, Resource, ResourceException, Resources
from lori.data.util import hash_data, resample
from lori.util import floor_date, parse_freq, slice_range, to_bool, to_timedelta, to_timezone

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal, LiteralString

except ImportError:
    from typing_extensions import Literal, LiteralString


class Retention:
    SECTION: str = "retention"

    _enabled: bool = False

    keep: str
    freq: str
    method: Optional[LiteralString]
    resample: Optional[str]
    timezone: tz.BaseTzInfo

    # noinspection PyShadowingNames
    @classmethod
    def build(cls, configs: Configurations, resource: Resource) -> Retentions:
        resource_configs = resource.get(cls.SECTION, default={})
        if isinstance(resource_configs, str):
            resource_configs = {"aggregate": resource_configs}
        elif isinstance(resource_configs, Dict):
            resource_configs["aggregate"] = resource.get("aggregate", default=None)
        else:
            raise ConfigurationException("Invalid retention method: " + str(resource_configs))

        configs = configs.copy()
        configs.update(resource_configs)
        defaults = {k: configs.pop(k) for k in list(configs.keys()) if k not in configs.sections}

        retentions = Retentions()
        for retain in [r for r in configs.sections if r not in ["databases"]]:
            retention = configs.get_section(retain, defaults=defaults)
            retain = retention.pop("keep", default=retain)
            retention = cls(retain, **retention)
            if retention.enabled:
                retentions.append(retention)
        return retentions

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        keep: str,
        freq: str = "D",
        resample: Optional[str] = None,
        aggregate: Optional[Literal["sum", "mean", "min", "max", "last"]] = None,
        timezone: Optional[tz.BaseTzInfo] = None,
        enabled: bool = True,
    ) -> None:
        self._logger = logging.getLogger(self.__module__)
        self._enabled = to_bool(enabled)

        if timezone is None:
            timezone = to_timezone(tzlocal.get_localzone_name())
        self.timezone = timezone

        # TODO: Implement timedelta sanity check
        self.keep = parse_freq(keep)
        self.freq = parse_freq(freq)
        self.resample = parse_freq(resample)

        if aggregate is not None:
            aggregate = aggregate.lower()
            if aggregate not in ["sum", "mean", "min", "max", "last"]:
                raise ConfigurationException(f"Invalid retention aggregation '{aggregate}'")
        self.method = aggregate

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Retention) and self._enabled == other._enabled and self._get_args() == other._get_args()
        )

    def __hash__(self) -> int:
        return hash((self._enabled, *self._get_args()))

    def __repr__(self) -> str:
        return f"{type(self).__name__}(keep={self.keep}, resample={self.resample}, aggregate={self.method})"

    def __str__(self) -> str:
        return (
            f"{type(self).__name__}:\n\t"
            + "\n\t".join(f"{k}={v}" for k, v in self._get_args().items())
            + f"\n\tenabled={self.enabled}"
        )

    # noinspection PyShadowingBuiltins
    def _get_args(self) -> Dict[str, Any]:
        return {
            "keep": self.keep,
            "freq": self.freq,
            "method": self.method,
            "resample": self.resample,
            "timezone": self.timezone,
        }

    @property
    def enabled(self) -> bool:
        return (
            self._enabled
            and self.keep is not None
            and self.freq is not None
            and self.method is not None
            and self.resample is not None
        )

    # noinspection PyProtectedMember, PyTypeChecker, PyShadowingBuiltins
    def aggregate(self, resources: Resources, full: bool = False) -> None:
        if not self.enabled:
            raise RetentionException("Retention aggregation disabled")

        retain = floor_date(pd.Timestamp.now(tz=self.timezone) - to_timedelta(self.keep), freq=self.freq)

        for database, database_resources in resources.groupby(lambda c: c.logger._connector):
            for _, resample_resources in database_resources.groupby(lambda c: c.group):
                start = database.read_first_index(resample_resources)
                if start is not None and start == floor_date(start, freq=self.freq):
                    start += pd.Timedelta(seconds=1)

                end = database.read_last_index(resample_resources)
                if end is not None:
                    end = min(floor_date(end, freq=self.freq), retain)

                if (any(t is None for t in [start, end]) or start >= end) and not full:
                    self._logger.debug(
                        f"Skip aggregating values of resource{'s' if len(resample_resources) > 1 else ''} "
                        + ", ".join([f"'{r.id}'" for r in resample_resources])
                        + " without any new values found"
                    )
                    continue

                if end - start < to_timedelta(self.freq) and not full:
                    self._logger.debug(
                        f"Skip aggregating data of resource{'s' if len(resample_resources) > 1 else ''} "
                        + ", ".join([f"'{r.id}'" for r in resample_resources])
                        + f" to {end.strftime('%d.%m.%Y (%H:%M:%S)')}"
                    )
                    continue

                self._logger.debug(
                    "Start aggregating data"
                    + f" of resource{'s' if len(resample_resources) > 1 else ''} "
                    + ", ".join([f"'{r.id}'" for r in resample_resources])
                    + f" up to {end.strftime('%d.%m.%Y (%H:%M:%S)')}"
                )

                resample_ranges = slice_range(start, end, timezone=self.timezone, freq=self.freq)
                resample_ranges.reverse()
                for resample_start, resample_end in resample_ranges:
                    self._logger.debug(
                        "Start resampling data"
                        + f" of resource{'s' if len(resample_resources) > 1 else ''} "
                        + ", ".join([f"'{r.id}'" for r in resample_resources])
                        + f" from {resample_start.strftime('%d.%m.%Y (%H:%M:%S)')}"
                        + f" to {resample_end.strftime('%d.%m.%Y (%H:%M:%S)')}"
                    )

                    data = database.read(resample_resources, start=resample_start, end=resample_end)
                    if data is None or data.empty:
                        self._logger.debug(
                            "Skipping empty resampling range"
                            + f" of resource{'s' if len(resample_resources) > 1 else ''} "
                            + ", ".join([f"'{r.id}'" for r in resample_resources])
                            + f" from {resample_start.strftime('%d.%m.%Y (%H:%M:%S)')}"
                            + f" to {resample_end.strftime('%d.%m.%Y (%H:%M:%S)')}"
                        )
                        continue

                    # errors = validate(data)
                    # if not errors.empty:
                    #     # TODO: Write detected errors into yaml file or smth similar
                    #     self._logger.warning(
                    #         f"Unable to aggregate data for resource{'s' if len(resample_resources) > 1 else ''} "
                    #         + ", ".join([f"'{r.id}'" for r in resample_resources])
                    #         + f"from {resample_start.strftime('%d.%m.%Y (%H:%M:%S)')} "
                    #         + f"to {resample_end.strftime('%d.%m.%Y (%H:%M:%S)')} "
                    #         + f"with {len(errors)} unprocessed errors"
                    #     )
                    #     print(errors)
                    #     continue

                    resampled_data = resample(data, self.resample, self.method)
                    if hash_data(resampled_data) != hash_data(data):
                        self._logger.debug(
                            f"Starting to resample {len(data)} to {len(resampled_data)} values"
                            + f" of resource{'s' if len(resample_resources) > 1 else ''} "
                            + ", ".join([f"'{r.id}'" for r in resample_resources])
                            + f" from {resample_start.strftime('%d.%m.%Y (%H:%M:%S)')}"
                            + f" to {resample_end.strftime('%d.%m.%Y (%H:%M:%S)')}"
                        )
                        database.delete(resample_resources, start=resample_start, end=resample_end)
                        database.write(resampled_data)

                        self._logger.info(
                            f"Resampled {len(data)} to {len(resampled_data)} values"
                            + f" of resource{'s' if len(resample_resources) > 1 else ''} "
                            + ", ".join([f"'{r.id}'" for r in resample_resources])
                            + f" from {resample_start.strftime('%d.%m.%Y (%H:%M:%S)')}"
                            + f" to {resample_end.strftime('%d.%m.%Y (%H:%M:%S)')}"
                        )
                    elif not not full:
                        self._logger.debug(
                            "Skipping already resampled range"
                            + f" of resource{'s' if len(resample_resources) > 1 else ''} "
                            + ", ".join([f"'{r.id}'" for r in resample_resources])
                            + f" from {resample_start.strftime('%d.%m.%Y (%H:%M:%S)')}"
                            + f" to {resample_end.strftime('%d.%m.%Y (%H:%M:%S)')}"
                        )
                    else:
                        self._logger.debug(
                            "Ending aggregation"
                            + f" of resource{'s' if len(resample_resources) > 1 else ''} "
                            + ", ".join([f"'{r.id}'" for r in resample_resources])
                            + " as data is already resampled"
                            + f" from {resample_start.strftime('%d.%m.%Y (%H:%M:%S)')}"
                            + f" to {resample_end.strftime('%d.%m.%Y (%H:%M:%S)')}"
                        )
                        break


class Retentions(MutableSequence[Retention]):
    def __init__(self, *retentions: Retention) -> None:
        self.__retentions = list(retentions)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(f'{r.keep}' for r in self.__retentions)})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(repr(r) for r in self.__retentions)

    def __contains__(self, retention: Retention):
        return self.contains(retention)

    def __getitem__(self, index: int) -> Retention:
        return self.__retentions[index]

    def __iter__(self) -> Iterator[Retention]:
        return iter(self.__retentions)

    def __len__(self) -> int:
        return len(self.__retentions)

    def __setitem__(self, index: int, retention: Retention) -> None:
        self.__retentions[index] = retention

    def __delitem__(self, index: int) -> None:
        del self.__retentions[index]

    def insert(self, index: int, retention: Retention):
        self.__retentions.insert(index, retention)

    def extend(self, retentions: Iterable[Retention], unique: bool = False):
        if unique:
            retentions = [r for r in retentions if r not in self.__retentions]
        self.__retentions.extend(retentions)

    def contains(self, retention: Retention):
        return any(r == retention for r in self.__retentions)

    def sort(self) -> None:
        def order(freq: str) -> int:
            freq_val = "".join(s for s in freq if s.isnumeric())
            freq_val = int(freq_val) if len(freq_val) > 0 else 1
            if freq.endswith("Y"):
                return 31535965 * freq_val
            elif freq.endswith("M"):
                return 2628000 * freq_val
            elif freq.endswith("W"):
                return 604800 * freq_val
            elif freq.endswith("D"):
                return 86400 * freq_val
            elif freq.endswith("h"):
                return 3600 * freq_val
            elif freq.endswith("min"):
                return 60 * freq_val
            elif freq.endswith("s"):
                return 1 * freq_val
            return 0

        self.__retentions = sorted(self.__retentions, key=lambda r: order(r.keep))


class RetentionException(ResourceException):
    """
    Raise if an error occurred while processing retention policy.

    """
