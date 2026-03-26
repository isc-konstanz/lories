# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``StringParameter`` — reads a configuration key as a plain string via
``configs.get()``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lories.core.configs.parameters.base import _TypedParameter

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations


class StringParameter(_TypedParameter):
    """Parameter for ``str`` configuration values.

    Reads the raw value from the config file without any type conversion —
    ``configs.get()`` already returns strings for scalar keys.

    Usage::

        class MyConfigurator(Configurator):
            host = StringParameter(default="localhost", desc="Broker hostname")
            topic = StringParameter(desc="MQTT topic to subscribe")
    """

    def _get_typed(self, configs: "_Configurations", key: str) -> str:
        return configs.get(key)

    def to_schema(self) -> dict:
        return {**super().to_schema(), "type": "str"}
