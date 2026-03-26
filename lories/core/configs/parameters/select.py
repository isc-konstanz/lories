# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.select
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``SelectParameter`` — a parameter whose value must be chosen from a fixed
list of options.  ``choices`` is the primary (first positional) argument,
making the intent explicit at the call site.

The accessor used to read the value is selected automatically from the
element type of *choices*, or can be overridden via *type*.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence, Type

import pandas as pd
from lories.core.configs.parameters.base import _UNSET, _TypedParameter

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations

# Re-use the same accessor map as the Parameter factory.
_ACCESSOR = {
    str: lambda cfg, k: cfg.get(k),
    int: lambda cfg, k: cfg.get_int(k),
    float: lambda cfg, k: cfg.get_float(k),
    bool: lambda cfg, k: cfg.get_bool(k),
    pd.Timestamp: lambda cfg, k: cfg.get_date(k),
}


class SelectParameter(_TypedParameter):
    """Parameter whose value must be one of a fixed list of *choices*.

    Unlike :class:`Parameter` (where *choices* is an optional constraint),
    here the list is the primary argument — it drives both validation **and**
    type-coercion.  The accessor is inferred from the element type of *choices*
    unless overridden by *type*.

    Usage::

        class MyConfigurator(Configurator):
            mode    = SelectParameter(["fast", "slow", "off"],
                                      default="fast", desc="Operating mode")
            channel = SelectParameter([1, 2, 3, 4], type=int,
                                      desc="Physical channel number")
            level   = SelectParameter([0.25, 0.5, 0.75, 1.0], type=float,
                                      default=1.0, desc="Output level")

    Parameters
    ----------
    choices:
        Non-empty sequence of allowed values.  The element type is used to
        infer the read accessor when *type* is not given explicitly.
    type:
        Override the accessor type.  Required when *choices* is empty or
        contains mixed types.
    """

    def __init__(
        self,
        choices: Sequence[Any],
        key: str | None = None,
        type: Type | None = None,  # noqa: A002
        default: Any = _UNSET,
        required: bool | None = None,
        desc: str | None = None,
        validator: Optional[Callable[[Any], None]] = None,
    ) -> None:
        if not choices:
            raise ValueError("SelectParameter requires at least one choice")

        # Infer type from the first element when not supplied explicitly.
        if type is None:
            type = builtins_type(choices[0])  # noqa: A001

        super().__init__(
            key=key,
            default=default,
            required=required,
            desc=desc,
            choices=[c.lower() for c in list(choices)],
            validator=validator,
        )
        resolved_type = type  # local alias avoids shadowing warning on dict.get
        self._accessor = _ACCESSOR.get(resolved_type, lambda cfg, k: cfg.get(k))  # type: ignore[arg-type]

    def _get_typed(self, configs: "_Configurations", key: str) -> Any:
        return self._accessor(configs, key)

    def to_schema(self) -> dict:
        # infer type name from the first choice element (same logic as __init__)
        type_name = builtins_type(self.choices[0]).__name__ if self.choices else "str"
        return {**super().to_schema(), "type": type_name}

    def __repr__(self) -> str:
        return (
            f"SelectParameter("
            f"name={self.name!r}, key={self._resolve_key()!r}, "
            f"choices={self.choices!r}, required={self.required}, "
            f"default={self.default!r})"
        )


# Alias for the built-in ``type()`` so the parameter named ``type`` does not shadow it inside __init__.
builtins_type = type
