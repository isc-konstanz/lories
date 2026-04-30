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
from lories.core.configs.parameters import Parameter, SelectParameter

# sudo lsusb -v | grep 'idVendor\|idProduct\|iProduct\|iSerial'
#   idVendor           0x10c4 Silicon Labs
#   idProduct          0xea60 CP210x UART Bridge
#   iProduct                2 CP2102 USB to UART Bridge Controller
#   iSerial                 3 0001

# "10c4:ea60"


# noinspection PyAbstractClass, SpellCheckingInspection
class _SerialConnector(Connector):
    _port = Parameter(key="port", type=str, required=True, desc="Serial device path (e.g. /dev/ttyUSB0)")
    _baudrate = Parameter(key="baudrate", type=int, default=9600, desc="Serial baudrate")
    _bytesize = Parameter(key="bytesize", type=int, default=serial.EIGHTBITS, desc="Number of data bits")
    _parity = SelectParameter(
        [serial.PARITY_NONE, serial.PARITY_EVEN, serial.PARITY_ODD, serial.PARITY_MARK, serial.PARITY_SPACE],
        key="parity",
        default=serial.PARITY_NONE,
        desc="Parity setting",
    )
    _stopbits = Parameter(key="stopbits", type=int, default=serial.STOPBITS_ONE, desc="Number of stop bits")
    _timeout = Parameter(key="timeout", type=float, default=3.0, desc="Read timeout (s)")
    _xonxoff = Parameter(key="xonxoff", type=bool, default=False, desc="Enable software flow control")
    _rtscts = Parameter(key="rtscts", type=bool, default=False, desc="Enable RTS/CTS hardware flow control")
    _dsrdtr = Parameter(key="dsrdtr", type=bool, default=False, desc="Enable DSR/DTR hardware flow control")

    _serial: Optional[serial.Serial]

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._serial = serial.Serial(
            baudrate=self._baudrate,
            bytesize=self._bytesize,
            parity=self._parity,
            stopbits=self._stopbits,
            timeout=self._timeout,
            xonxoff=self._xonxoff,
            rtscts=self._rtscts,
            dsrdtr=self._dsrdtr,
        )
        self._serial.port = self._port

    def connect(self, resources: Resources) -> None:
        try:
            self._serial.open()
        except SerialException as e:
            raise ConnectionError(self, f"Failed to open serial port {self._serial.port}: {e}")

    def disconnect(self) -> None:
        try:
            if self.is_connected():
                self._serial.close()
        except SerialException as e:
            raise ConnectionError(self, f"Failed to close serial port {self._serial.port}: {e}")

    def is_connected(self) -> bool:
        return self._serial is not None and self._serial.is_open

    def _write_string(self, data: AnyStr, encode="ascii") -> None:
        try:
            self._serial.write(data.encode(encode))
        except SerialException as e:
            raise ConnectionError(self, f"Failed to write to serial port {self._serial.port}: {e}")

    def _read_line(self, decode="ascii") -> AnyStr:
        try:
            return self._serial.readline().decode(decode, errors="ignore").replace("\x00", "").strip()
        except SerialException as e:
            raise ConnectionError(self, f"Failed to read from serial port {self._serial.port}: {e}")
