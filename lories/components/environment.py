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
    """
    The Environment class serves as an environmental monitoring component in the lories framework.
    This class encapsulates data from environmental sensors, presenting them as constants for easy access.
    - temperature (float): The current temperature reading.
    - humidity (float): The current humidity level.
    - pressure (float): The current atmospheric pressure reading.

    """

    TEMPERATUR = Constant(float, "temperature", "Temperature", "Celsius")
    HUMIDITY = Constant(float, "humidity", "Humidity", "%")
    PRESSURE = Constant(float, "pressure", "Pressure", "Pa")
