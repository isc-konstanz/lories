# -*- coding: utf-8 -*-
"""
lories.data.converters.io.voltage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories.core.typing import Configurations
from lories.data.converters import register_converter_type
from lories.data.converters.io.analog import AnalogInput


# noinspection PyAbstractClass
@register_converter_type("voltage_input", "voltage_sensor")
class VoltageInput(AnalogInput):
    INPUT_KEY: str = "voltage"

    def _configure_input(
        self,
        configs: Configurations,
        default_max: float = None,
        default_min: float = 0.0,
    ) -> None:
        super()._configure_input(configs, configs.get_float("voltage", default=default_max), default_min)
