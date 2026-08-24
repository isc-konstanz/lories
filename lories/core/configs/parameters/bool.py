# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.bool
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``BoolParameter`` — reads a configuration key as a ``bool`` via
``configs.get_bool()``, which internally uses ``lories.util.to_bool``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lories.core.configs.parameters.base import _TypedParameter

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations


class BoolParameter(_TypedParameter):
    """Parameter for ``bool`` configuration values.

    Delegates to ``configs.get_bool()`` which uses ``lories.util.to_bool``
    internally, so string representations like ``"true"``, ``"yes"``,
    ``"false"``, ``"no"`` are handled transparently.

    Usage::

        class MyConfigurator(Configurator):
            enabled = BoolParameter(default=True, desc="Enable this configurator")
            tls = BoolParameter(default=False, desc="Use TLS encryption")
    """

    def _get_typed(self, configs: "_Configurations", key: str) -> bool:
        return configs.get_bool(key)

    def to_schema(self) -> dict:
        return {**super().to_schema(), "type": "bool"}
