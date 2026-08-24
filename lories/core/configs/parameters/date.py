# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.date
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``DateParameter`` — reads a configuration key as a ``pd.Timestamp`` via
``configs.get_date()``, which internally uses ``lories.util.to_date``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd
from lories.core.configs.parameters.base import _UNSET, _TypedParameter

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations


class DateParameter(_TypedParameter):
    """Parameter for ``pd.Timestamp`` / date configuration values.

    Delegates to ``configs.get_date()`` which uses ``lories.util.to_date``
    internally, so ISO-8601 strings, Unix timestamps (int) and ``datetime``
    objects are all accepted.  An optional *timezone* is forwarded to
    ``configs.get_date()``.

    Usage::

        class MyConfigurator(Configurator):
            start = DateParameter(desc="Start date of the data range")
            end   = DateParameter(required=False, desc="Optional end date")
    """

    def __init__(
        self,
        key: str | None = None,
        default: Any = _UNSET,
        required: bool | None = None,
        desc: str | None = None,
        choices=None,
        validator=None,
        timezone=None,
    ) -> None:
        super().__init__(
            key=key,
            default=default,
            required=required,
            desc=desc,
            choices=choices,
            validator=validator,
        )
        self.timezone = timezone

    def _get_typed(self, configs: "_Configurations", key: str) -> pd.Timestamp:
        kwargs = {}
        if self.timezone is not None:
            kwargs["timezone"] = self.timezone
        return configs.get_date(key, **kwargs)

    def to_schema(self) -> dict:
        schema = super().to_schema()
        schema["type"] = "datetime"
        if self.timezone is not None:
            schema["timezone"] = str(self.timezone)
        return schema
