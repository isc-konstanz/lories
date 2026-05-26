# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.numeric
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``_NumericParameter`` — intermediate base for ``IntParameter`` and
``FloatParameter``.  Adds inclusive ``min`` / ``max`` range bounds on top of
the ``choices`` and ``validator`` already provided by ``_TypedParameter``.
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional, Union

from lories.core.configs.parameters.base import _UNSET, _config_error, _TypedParameter


class _NumericParameter(_TypedParameter):
    """Intermediate base for numeric (``int`` / ``float``) parameter descriptors.

    Extends :class:`_TypedParameter` with inclusive ``min`` / ``max`` bounds
    that are checked **after** type coercion and **before** the custom
    ``validator``.  Both bounds are optional and independent.

    Usage (via the concrete subclasses)::

        port    = IntParameter(default=1883, min=1, max=65535, desc="Broker port")
        ratio   = FloatParameter(default=1.0, min=0.0, max=1.0, desc="Ratio [0-1]")
        timeout = IntParameter(min=1, desc="Timeout in seconds (≥ 1)")
    """

    def __init__(
        self,
        key: str | None = None,
        default: Any = _UNSET,
        required: bool | None = None,
        desc: str | None = None,
        choices: Optional[List[Any]] = None,
        validator: Optional[Callable[[Any], None]] = None,
        min: Optional[Union[int, float]] = None,  # noqa: A002
        max: Optional[Union[int, float]] = None,  # noqa: A002
    ) -> None:
        super().__init__(
            key=key,
            default=default,
            required=required,
            desc=desc,
            choices=choices,
            validator=validator,
        )
        self.min = min
        self.max = max

    def _validate(self, key: str, value: Any) -> None:
        super()._validate(key, value)
        if self.min is not None and value < self.min:
            raise _config_error(f"Value {value!r} for '{key}' is below the minimum of {self.min!r}")
        if self.max is not None and value > self.max:
            raise _config_error(f"Value {value!r} for '{key}' exceeds the maximum of {self.max!r}")

    def to_schema(self) -> dict:
        return {**super().to_schema(), "min": self.min, "max": self.max}

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
