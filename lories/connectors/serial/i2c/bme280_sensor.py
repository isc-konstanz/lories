# -*- coding: utf-8 -*-
"""
lories.connectors.i2c.bme280
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

try:
    import bme280
except ImportError:

    class BME280Results:
        def __init__(self):
            self.temperature = 0
            self.humidity = 0
            self.pressure = 0

    class BME280Mock:
        @staticmethod
        def load_calibration_params(bus, addr):
            print(f"BME280 mock calibration: addr: {addr}")
            return {}

        @staticmethod
        def sample(bus, address, calibration_params):
            print(f"BME280 mock sample: address: {address}, calibration_params: {calibration_params}")
            return BME280Results()

    bme280 = BME280Mock

from lories.components.environment import Environment


# noinspection SpellCheckingInspection
class Bme280Sensor:
    INCLUDES = [Environment.TEMPERATUR, Environment.HUMIDITY, Environment.PRESSURE]

    def __init__(self, bus, address: int) -> None:
        self.calibration_params = bme280.load_calibration_params(bus, address)
        self.address = address

    def read(self, bus) -> dict:
        data = bme280.sample(bus, self.address, self.calibration_params)
        return {
            Environment.TEMPERATUR: data.temperature,
            Environment.HUMIDITY: data.humidity,
            Environment.PRESSURE: data.pressure,
        }
