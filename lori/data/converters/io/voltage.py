# -*- coding: utf-8 -*-
"""
lori.data.converters.io.voltage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lori import Configurations
from lori.data.converters import register_converter_type
from lori.data.converters.io.analog import AnalogInput


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
