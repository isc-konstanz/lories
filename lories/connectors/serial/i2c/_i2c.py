# -*- coding: utf-8 -*-
"""
lories.connectors.i2c._i2c
~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from typing import Optional

from smbus2 import SMBus

from lories.connectors import ConnectionError, Connector
from lories.core.configs.parameters import Parameter
from lories.typing import Resources


# noinspection PyAbstractClass
class _I2CConnector(Connector):
    _port = Parameter(key="port", type=int, default=1, desc="I2C bus number (e.g. 1 for /dev/i2c-1)")

    _bus: Optional[SMBus]
    _port: int

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._bus = None

    def connect(self, resources: Resources) -> None:
        try:
            self._bus = SMBus(self._port)
        except Exception as e:
            raise ConnectionError(self, f"Failed to open I2C bus {self._port}: {e}") from e

    def disconnect(self) -> None:
        if self.is_connected():
            self._bus.close()
            self._bus = None

    def is_connected(self) -> bool:
        return self._bus is not None

    def get_bus(self):
        if not self.is_connected():
            raise ConnectionError(self, "I2C bus not connected")
        return self._bus
