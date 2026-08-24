# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.integer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``IntParameter`` — reads a configuration key as an ``int`` via
``configs.get_int()``, which internally uses ``lories.util.to_int``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lories.core.configs.parameters.numeric import _NumericParameter

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations


class IntParameter(_NumericParameter):
    """Parameter for ``int`` configuration values.

    Delegates to ``configs.get_int()`` which uses ``lories.util.to_int``
    internally, so string representations like ``"42"`` are handled
    transparently.  Supports optional inclusive ``min`` / ``max`` bounds.

    Usage::

        class MyConfigurator(Configurator):
            port    = IntParameter(default=1883, min=1, max=65535, desc="Broker port")
            timeout = IntParameter(default=30, min=1, desc="Connection timeout in seconds")
    """

    def _get_typed(self, configs: "_Configurations", key: str) -> int:
        return configs.get_int(key)

    def to_schema(self) -> dict:
        return {**super().to_schema(), "type": "int"}
