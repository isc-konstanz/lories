# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.float
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``FloatParameter`` — reads a configuration key as a ``float`` via
``configs.get_float()``, which internally uses ``lories.util.to_float``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lories.core.configs.parameters.numeric import _NumericParameter

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations


class FloatParameter(_NumericParameter):
    """Parameter for ``float`` configuration values.

    Delegates to ``configs.get_float()`` which uses ``lories.util.to_float``
    internally, so integer or string representations are handled
    transparently.  Supports optional inclusive ``min`` / ``max`` bounds.

    Usage::

        class MyConfigurator(Configurator):
            threshold = FloatParameter(default=0.5, min=0.0, max=1.0, desc="Alert threshold [0-1]")
            factor    = FloatParameter(min=0.0, desc="Scaling factor (≥ 0)")
    """

    def _get_typed(self, configs: "_Configurations", key: str) -> float:
        return configs.get_float(key)

    def to_schema(self) -> dict:
        return {**super().to_schema(), "type": "float"}
