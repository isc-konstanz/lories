# -*- coding: utf-8 -*-
"""
lories.connectors.i2c._i2c
~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from typing import List, Optional

from lories.connectors import ConnectionError, Connector
from lories.core import Configurations
from lories.core.configs.parameters import Parameter
from lories.typing import Resources

try:
    from smbus2 import SMBus
except (ImportError, OSError):

    class SMBus:
        """
        Minimal mock replacement for smbus2.SMBus
        Works on Windows and in dev environments.
        """

        def __init__(self, *args, **kwargs):
            pass

        @staticmethod
        def write_i2c_block_data(addr: int, register: int, data: List[int]) -> None:
            print(f"I2C mock write: addr: {addr}, register: {register}, data: {data}")

        @staticmethod
        def read_i2c_block_data(addr: int, register: int, length: int) -> List[int]:
            print(f"I2C mock read: addr: {addr}, register: {register}, length: {length}")
            return [0 for _ in range(length)]

        @staticmethod
        def close() -> None:
            print("I2C mock close")


# noinspection PyAbstractClass
class _I2CConnector(Connector):
    _port = Parameter(key="port", type=int, default=1, desc="I2C bus number (e.g. 1 for /dev/i2c-1)")

    _bus: Optional[SMBus]
    _bus_number: int
    _port: int

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self._bus = None

    def connect(self, resources: Resources) -> None:
        try:
            self._bus = SMBus(self._port)
        except Exception as e:
            raise ConnectionError(f"Failed to open I2C bus {self._bus_number}: {e}") from e

    def disconnect(self) -> None:
        if self.is_connected():
            self._bus.close()
            self._bus = None

    def is_connected(self) -> bool:
        return self._bus is not None

    def get_bus(self):
        if not self.is_connected():
            raise ConnectionError("I2C bus not connected")
        return self._bus
