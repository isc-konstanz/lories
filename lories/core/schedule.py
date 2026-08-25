# -*- coding: utf-8 -*-
"""
lories.core.schedule
~~~~~~~~~~~~~~~~~~~~

Wall-clock tick-schedule vocabulary: ``interval`` (minutes) is the cadence a
schedule is aligned to, ``offset`` (minutes, ``0 <= offset < interval``)
shifts that alignment within the interval, and ``intake_delay`` clips how
close to *now* input data is consumed, so late-arriving records (logger
flushes, database replication lag) land before the processing frontier
passes them.

Alignment is absolute (``floor_date`` on the timezone plus the offset), not
relative to any activation time, so restarts never shift the schedule.

This module is the one home for the vocabulary. ``WeatherForecast`` uses it
in lories; the sparcs ``FieldSimulation``/``SoilPredictor`` carry a private
pre-lift copy (``simulation/_schedule.py``, bit-identical slot expressions)
to be migrated here after the branch merges - note the parse raises
``ConfigurationError`` where the pre-lift copy raised ``ValueError``.
"""

from __future__ import annotations

import pandas as pd
from lories.core.configs.configurator import Configurator, ConfiguratorMeta
from lories.core.configs.errors import ConfigurationError
from lories.core.configs.parameters import DurationParameter
from lories.util import floor_date, to_timedelta

__all__ = [
    "TickSchedule",
    "validate_tick_schedule",
    "validate_intake_delay",
    "parse_tick_schedule",
    "parse_intake_delay",
    "slot_ceil",
    "slot_floor",
]


def validate_tick_schedule(
    interval: pd.Timedelta,
    offset: pd.Timedelta,
    *,
    section_name: str = "schedule",
) -> tuple[pd.Timedelta, pd.Timedelta]:
    """Validate bound ``interval``/``offset`` durations.

    Descriptor-based consumers declare ``interval``/``offset`` as
    DurationParameters (so the config system recognizes and documents them)
    and call this for the constraints descriptors cannot express:
    ``interval > 0`` and ``0 <= offset < interval``. Calendar units
    (``relativedelta``: weeks/months/years) are rejected - slot alignment
    needs a fixed duration. ``section_name`` only names the config section in
    the raised ``ConfigurationError`` messages.
    """
    for key, value in (("interval", interval), ("offset", offset)):
        if not isinstance(value, pd.Timedelta):
            raise ConfigurationError(
                f"[{section_name}] {key} must be a fixed duration (no calendar units), got {value!r}"
            )
    if interval <= pd.Timedelta(0):
        raise ConfigurationError(f"[{section_name}] interval must be positive, got {interval}")
    if not pd.Timedelta(0) <= offset < interval:
        raise ConfigurationError(f"[{section_name}] offset must be in [0, interval), got {offset}")
    return interval, offset


def parse_tick_schedule(
    configs,
    *,
    default_interval: int,
    default_offset: int = 0,
    section_name: str = "schedule",
) -> tuple[pd.Timedelta, pd.Timedelta]:
    """Parse and validate ``interval``/``offset`` (integer MINUTES) from raw ``configs``.

    Compatibility path for pre-descriptor callers whose config vocabulary is
    integer minutes (the sparcs soil-simulation copy, ``WeatherForecast``);
    descriptor-based consumers declare DurationParameters and use
    ``validate_tick_schedule`` on the bound values instead.
    """
    interval = pd.Timedelta(minutes=int(configs.get("interval", default=default_interval)))
    offset = pd.Timedelta(minutes=int(configs.get("offset", default=default_offset)))
    return validate_tick_schedule(interval, offset, section_name=section_name)


def validate_intake_delay(delay: pd.Timedelta, *, key: str = "intake_delay") -> pd.Timedelta:
    """Validate a bound intake delay (``0`` = consume input up to now).

    A processing tick at slot ``t`` consumes input data up to ``t - delay``;
    the delay must cover the worst-case lag of the input path (logger flush
    interval, replication cadence), or late records are skipped forever once
    the frontier passes them.
    """
    if delay < pd.Timedelta(0):
        raise ConfigurationError(f"'{key}' must not be negative, got {delay}")
    return delay


def parse_intake_delay(
    configs,
    *,
    key: str = "intake_delay",
    default: str = "0min",
) -> pd.Timedelta:
    """Parse the intake delay from raw ``configs`` (compatibility path)."""
    value = configs.get(key, default=default)
    try:
        delay = to_timedelta(value)
    except ValueError as exc:
        raise ConfigurationError(f"Invalid '{key}' duration: {value!r}") from exc
    return validate_intake_delay(delay, key=key)


def slot_ceil(now: pd.Timestamp, tz, interval: pd.Timedelta, offset: pd.Timedelta) -> pd.Timestamp:
    """First aligned slot strictly after ``now``."""
    slot = floor_date(now, tz, freq=f"{int(interval.total_seconds())}s")
    slot += offset
    while slot <= now:
        slot += interval
    return slot


def slot_floor(now: pd.Timestamp, tz, interval: pd.Timedelta, offset: pd.Timedelta) -> pd.Timestamp:
    """Most-recent aligned slot at or before ``now``."""
    boundary = floor_date(now, tz, freq=f"{int(interval.total_seconds())}s") + offset
    if boundary > now:
        boundary -= interval
    return boundary


class TickSchedule(metaclass=ConfiguratorMeta):
    """Declarative tick-schedule parameters for duration-based config surfaces.

    Mix in BEFORE the Configurator base (``class MyComponent(TickSchedule,
    Component)``) to declare ``interval``/``offset``/``intake_delay`` as
    DurationParameters and get the cross-field validation on configure,
    instead of every component re-declaring them. Uses ``ConfiguratorMeta``
    so the declared parameters are inherited into the consuming class's
    ``__config_parameters__``. The wrong mix-in order fails at class
    definition; validation runs in ``_at_configure`` (right after the
    parameters bind, before the consumer's ``configure`` body), so a
    consumer forgetting ``super().configure()`` cannot skip it.

    Int-minute legacy surfaces (``WeatherForecast``, the sparcs soil copy)
    keep their own declarations; this mixin is for new, duration-typed
    config surfaces.
    """

    interval = DurationParameter(
        key="interval",
        default="1h",
        min="1min",
        desc="Schedule interval (wall-clock aligned cadence)",
    )
    offset = DurationParameter(
        key="offset",
        default="0min",
        min="0min",
        desc="Schedule offset within the interval",
    )
    intake_delay = DurationParameter(
        key="intake_delay",
        default="0min",
        min="0min",
        desc="Input newer than now minus this delay waits for the next slot, covering logger/replication lag",
    )

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        mro = list(cls.__mro__)
        if Configurator in mro and mro.index(TickSchedule) > mro.index(Configurator):
            raise TypeError(
                f"{cls.__name__} must mix TickSchedule in BEFORE the Configurator base, "
                f"e.g. class {cls.__name__}(TickSchedule, Component): with the reversed "
                "order the mixin's configure-time validation is silently skipped"
            )

    # noinspection PyUnresolvedReferences
    def _at_configure(self, configs) -> None:
        super()._at_configure(configs)
        validate_tick_schedule(self.interval, self.offset, section_name=getattr(self, "TYPE", "schedule"))
        validate_intake_delay(self.intake_delay)

    def next_slot(self, now: pd.Timestamp, tz="UTC") -> pd.Timestamp:
        """First aligned slot strictly after ``now``."""
        return slot_ceil(now, tz, self.interval, self.offset)

    def last_slot(self, now: pd.Timestamp, tz="UTC") -> pd.Timestamp:
        """Most-recent aligned slot at or before ``now``."""
        return slot_floor(now, tz, self.interval, self.offset)

    def intake_cutoff(self, slot: pd.Timestamp) -> pd.Timestamp:
        """Input consumption cutoff for a tick at ``slot``."""
        return slot - self.intake_delay
