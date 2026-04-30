# -*- coding: utf-8 -*-
"""
lories.connectors.i2c.i2c
~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from typing import List

import pandas as pd
from lories._core import ChannelState  # noqa
from lories.connectors import ConnectionError, register_connector_type
from lories.connectors.serial.i2c._i2c import _I2CConnector
from lories.connectors.serial.i2c.bme280_sensor import Bme280Sensor
from lories.core.configs.parameters import ChannelParameter
from lories.typing import Resources


@register_connector_type("i2c")
class I2CConnector(_I2CConnector):
    # Per-channel parameters
    sensor = ChannelParameter(
        type=str, required=False, choices=["bme280"], desc="Sensor type (e.g. 'bme280'); omit for raw register access"
    )
    address = ChannelParameter(type=int, required=False, desc="I2C device address")
    register = ChannelParameter(type=int, required=False, desc="Register address (raw mode)")
    length = ChannelParameter(type=int, required=False, default=1, desc="Number of bytes to read (raw mode)")
    measurement = ChannelParameter(
        type=str, required=False, desc="Sensor measurement key (e.g. 'temperature', 'humidity', 'pressure')"
    )

    _sensors: dict

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        self._sensors = {}

        for sensor, sensor_resources in resources.groupby("sensor"):
            for address, address_resources in sensor_resources.groupby("address"):
                if sensor == "bme280":
                    self._sensors[f"{sensor}_{address}"] = Bme280Sensor(self.get_bus(), address)
                elif sensor is None:
                    pass

    def _read_sensor(
        self, sensor: str, address: int, resources: Resources, timestamp, results: pd.DataFrame
    ) -> pd.DataFrame:
        if sensor.lower() == "bme280":
            bme280_sensor = self._sensors[f"{sensor}_{address}"]
            try:
                bme280_results = bme280_sensor.read(self.get_bus())
                for measurement, measurement_resources in resources.groupby("measurement"):
                    if measurement not in bme280_sensor.INCLUDES:
                        raise ConnectionError(f"Measurement {measurement} not found in {bme280_sensor.INCLUDES}")

                    for channel in measurement_resources:
                        results.at[timestamp, channel.id] = bme280_results[measurement]

            except Exception as e:
                self._logger.warning(f"Failed to read sensor {sensor}: {e}")

        elif sensor is None:
            for resource in resources:
                register = resource.get("register")
                if register is None:
                    raise ConnectionError(f"Missing register for resource {resource}")

                length = resource.get("length", default=1)

                try:
                    raw = self._read_bytes(0, register, length)

                    # Default interpretation: big-endian integer
                    value = 0
                    for byte in raw:
                        value = (value << 8) | byte

                    results[resource.id] = value

                except Exception as e:
                    self._logger.warning(f"Failed to read register 0x{register:02X}: {e}")

        else:
            raise ConnectionError(f"Unknown sensor {sensor}")

        return results

    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.utcnow().floor("s")
        results = pd.DataFrame(index=[timestamp], columns=resources.ids)

        for sensor, sensor_resources in resources.groupby("sensor"):
            for address, address_resources in sensor_resources.groupby("address"):
                results = self._read_sensor(sensor, address, address_resources, timestamp, results)

        return results

    def write(self, data: pd.DataFrame) -> None:
        """
        Write values to registers.
        Requires resource to define 'register'.
        """

        if data.empty:
            return

        for column in data.columns:
            resource = self._resources.get(column)
            if resource is None:
                continue

            register = getattr(resource, "register", None)
            if register is None:
                self._logger.warning(f"Missing register for resource {column}")
                continue

            value = int(data.iloc[-1][column])

            # Default: write as single byte
            try:
                self._write_bytes(0, register, [value & 0xFF])

            except Exception as e:
                self._logger.warning(f"Failed to write register 0x{register:02X}: {e}")

    def _write_bytes(self, address: int, register: int, data: List[int]) -> None:
        if not self.is_connected():
            raise ConnectionError("I2C bus not connected")

        try:
            self._bus.write_i2c_block_data(address, register, data)

        except Exception as e:
            raise ConnectionError(f"I2C write failed " f"(addr=0x{address:02X}, reg=0x{register:02X}): {e}") from e

    def _read_bytes(self, address: int, register: int, length: int) -> List[int]:
        if not self.is_connected():
            raise ConnectionError("I2C bus not connected")

        try:
            return self._bus.read_i2c_block_data(address, register, length)

        except Exception as e:
            raise ConnectionError(f"I2C read failed " f"(addr=0x{address:02X}, reg=0x{register:02X}): {e}") from e
