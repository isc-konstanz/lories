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
    An environment component bundles ambient measurements — temperature, relative humidity, and
    atmospheric pressure — into a single logical sensor exposed to the rest of the system. It is
    typically backed by a multi-channel sensor connector (e.g. BME280 over I2C) and serves as the
    canonical source of local atmospheric conditions for downstream components such as weather
    correction, photovoltaic models, or HVAC controllers.
    """

    TEMPERATUR = Constant(float, "temperature", "Temperature", "Celsius")
    HUMIDITY = Constant(float, "humidity", "Humidity", "%")
    PRESSURE = Constant(float, "pressure", "Pressure", "Pa")
