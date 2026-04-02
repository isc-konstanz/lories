# -*- coding: utf-8 -*-
"""
lories.connectors.i2c._i2c
~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from typing import Optional

from smbus2 import SMBus

from lories.connectors import ConnectionError, Connector
from lories.core import Configurations
from lories.typing import Resources


# noinspection PyAbstractClass
class _I2CConnector(Connector):
    _bus: Optional[SMBus]
    _bus_number: int
    _port: int

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        self._bus = None
        self._port = configs.get_int("port", default=1)

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
