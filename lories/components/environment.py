# -*- coding: utf-8 -*-
"""
lories.components.environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from lories import Constant
from lories.components import Component, register_component_type


@register_component_type("environment")
class Environment(Component):
    TEMPERATUR = Constant(float, "temperature", "Temperature", "Celsius")
    HUMIDITY = Constant(float, "humidity", "Humidity", "%")
    PRESSURE = Constant(float, "pressure", "Pressure", "Pa")
