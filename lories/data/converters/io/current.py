# -*- coding: utf-8 -*-
"""
lories.data.converters.io.current
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories.core import ConfigurationError
from lories.core.typing import Configurations
from lories.data.converters import register_converter_type
from lories.data.converters.io.analog import AnalogInput


# noinspection PyAbstractClass
@register_converter_type("current_input", "current_sensor")
class CurrentInput(AnalogInput):
    INPUT_KEY: str = "current"


# noinspection PyAbstractClass
@register_converter_type("current_loop", "4_20ma_input")
class CurrentLoopInput(AnalogInput):
    def _configure_input(
        self,
        configs: Configurations,
        default_max: float = 0.02,
        default_min: float = 0.004,
    ) -> None:
        super()._configure_input(configs, default_max, default_min)

    # noinspection PyShadowingBuiltins
    def _assert_input(self, min: float, max: float, zero: float) -> None:
        super()._assert_input(min, max, zero)
        if min < 0.004:
            raise ConfigurationError(f"Invalid current minimum for '{self.id}' 4-20mA input: {min}")
        if max > 0.02:
            raise ConfigurationError(f"Invalid current maximum for '{self.id}' 4-20mA input: {max}")
