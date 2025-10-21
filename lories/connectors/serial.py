# -*- coding: utf-8 -*-
"""
lories.connectors.serial
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import serial
import pandas as pd
from typing import Optional

from lories.core import Configurations, Resources
from lories.connectors import Connector

# noinspection SpellCheckingInspection
class SerialConnector(Connector):
    """
    Serial connector
    """

    port: str
    baudrate: int
    bytesize: int
    parity: str
    stopbits: int
    timeout: float
    xonxoff: bool
    rtscts: bool
    dsrdtr: bool

    _serial: Optional[serial.Serial]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._serial = None

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        # TBS03 Datasheet: Set the COM speed to 19200, 8 Bits, No Parity, 1 Stop Bit, No Handshake.
        self.port = configs.get("port", default="/dev/ttyUSB0")
        self.baudrate = configs.get_int("baudrate", default=9600)
        self.bytesize =configs.get_int("bytesize", default=serial.EIGHTBITS)
        self.parity = configs.get("parity", default=serial.PARITY_NONE)
        self.stopbits = configs.get_int("stopbits", default=serial.STOPBITS_ONE)
        self.timeout = configs.get_float("timeout", default=2.0)
        self.xonxoff = configs.get_bool("xonxoff", default=False)
        self.rtscts = configs.get_bool("rtscts", default=False)
        self.dsrdtr = configs.get_bool("dsrdtr", default=False)

    def connect(self, resources: Resources) -> None:
        """Open the serial port."""
        self._serial = serial.Serial(
            port=self.port,
            baudrate=self.baudrate,
            bytesize=self.bytesize,
            parity=self.parity,
            stopbits=self.stopbits,
            timeout=self.timeout,
            xonxoff=self.xonxoff,
            rtscts=self.rtscts,
            dsrdtr=self.dsrdtr,
        )

    def disconnect(self) -> None:
        if self._serial and self._serial.is_open:
            self._serial.close()
            self._serial = None

    def is_connected(self) -> bool:
        return self._serial is not None and self._serial.is_open

    def read(self, resources: Resources) -> pd.DataFrame:
        raise NotImplementedError("SerialConnector does not support reading data")

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("SerialConnector does not support writing data")
