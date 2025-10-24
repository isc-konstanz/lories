# -*- coding: utf-8 -*-
"""
lories.connectors.serial._serial
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import AnyStr, Optional

import serial
from serial import SerialException

from lories.connectors import ConnectionError, Connector
from lories.core import Configurations, Resources


# noinspection PyAbstractClass, SpellCheckingInspection
class _SerialConnector(Connector):
    _serial: Optional[serial.Serial]

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._serial = serial.Serial(
            port=configs.get("port"),
            baudrate=configs.get_int("baudrate", default=9600),
            bytesize=configs.get_int("bytesize", default=serial.EIGHTBITS),
            parity=configs.get("parity", default=serial.PARITY_NONE),
            stopbits=configs.get_int("stopbits", default=serial.STOPBITS_ONE),
            timeout=configs.get_float("timeout", default=3),
            xonxoff=configs.get_bool("xonxoff", default=False),
            rtscts=configs.get_bool("rtscts", default=False),
            dsrdtr=configs.get_bool("dsrdtr", default=False),
        )

    def connect(self, resources: Resources) -> None:
        try:
            self._serial.open()

        except SerialException as e:
            raise ConnectionError(f"Failed to open serial port {self._serial.port}: {e}") from e

    def disconnect(self) -> None:
        if self.is_connected():
            self._serial.close()

    def is_connected(self) -> bool:
        return self._serial is not None and self._serial.is_open

    def _write_string(self, data: AnyStr, encode="ascii") -> None:
        self._serial.write(data.encode(encode))

    def _read_line(self, decode="ascii") -> AnyStr:
        return self._serial.readline().decode(decode, errors="ignore").strip()
