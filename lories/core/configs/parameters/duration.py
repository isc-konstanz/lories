# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.duration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``DurationParameter`` — reads a configuration key as a ``pd.Timedelta`` (or
``dateutil.relativedelta`` for calendar units) via ``configs.get_duration()``,
which internally uses ``lories.util.to_timedelta``.
"""

from __future__ import annotations

from dateutil.relativedelta import relativedelta
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Union

import pandas as pd
from lories.core.configs.parameters.base import _UNSET, _config_error, _TypedParameter
from lories.util import to_timedelta

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations


Duration = Union[pd.Timedelta, relativedelta]


def _coerce(value: Any) -> Optional[Duration]:
    if value is None:
        return None
    if isinstance(value, (pd.Timedelta, relativedelta)):
        return value
    return to_timedelta(value)


class DurationParameter(_TypedParameter):
    """Parameter for duration / frequency configuration values.

    Delegates to ``configs.get_duration()`` which uses ``lories.util.to_timedelta``
    internally, so short forms like ``"10s"``, ``"10min"``, ``"1h"``, ``"1D"``,
    ``"1W"``, ``"1M"``, ``"1Y"`` (and spelled-out variants) are accepted.
    Fixed units (``s``, ``min``, ``h``, ``D``) resolve to ``pd.Timedelta``;
    calendar units (``W``, ``M``, ``Y``) resolve to ``relativedelta``.

    Supports inclusive ``min`` / ``max`` bounds.  Bounds, default, and choices
    accept the same string forms as the config value — they are parsed once at
    descriptor construction time.

    Comparing a calendar ``relativedelta`` against a fixed ``pd.Timedelta``
    bound raises ``ConfigurationError`` with a clear message.

    Usage::

        class MyConfigurator(Configurator):
            poll     = DurationParameter(default="10s", min="1s", desc="Poll interval")
            backfill = DurationParameter(default="1M", desc="Backfill window")
    """

    def __init__(
        self,
        key: str | None = None,
        default: Any = _UNSET,
        required: bool | None = None,
        desc: str | None = None,
        choices: Optional[List[Any]] = None,
        validator: Optional[Callable[[Any], None]] = None,
        min: Optional[Any] = None,  # noqa: A002
        max: Optional[Any] = None,  # noqa: A002
    ) -> None:
        if default is not _UNSET:
            default = _coerce(default)
        if choices is not None:
            choices = [_coerce(c) for c in choices]
        super().__init__(
            key=key,
            default=default,
            required=required,
            desc=desc,
            choices=choices,
            validator=validator,
        )
        self.min = _coerce(min)
        self.max = _coerce(max)

    def _get_typed(self, configs: "_Configurations", key: str) -> Duration:
        return configs.get_duration(key)

    def _validate(self, key: str, value: Any) -> None:
        super()._validate(key, value)
        try:
            if self.min is not None and value < self.min:
                raise _config_error(f"Value {value!r} for '{key}' is below the minimum of {self.min!r}")
            if self.max is not None and value > self.max:
                raise _config_error(f"Value {value!r} for '{key}' exceeds the maximum of {self.max!r}")
        except TypeError as exc:
            raise _config_error(
                f"Cannot bound duration {value!r} for '{key}' against "
                f"[{self.min!r}, {self.max!r}] — mixing calendar and fixed durations"
            ) from exc

    def to_schema(self) -> dict:
        return {**super().to_schema(), "type": "timedelta", "min": self.min, "max": self.max}

    def __repr__(self) -> str:
        range_str = ""
        if self.min is not None or self.max is not None:
            range_str = f", range=[{self.min!r}, {self.max!r}]"
        return (
            f"{type(self).__name__}("
            f"name={self.name!r}, key={self._resolve_key()!r}, "
            f"required={self.required}, default={self.default!r}"
            f"{range_str})"
        )
