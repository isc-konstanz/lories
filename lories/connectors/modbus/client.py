# -*- coding: utf-8 -*-
"""
lories.connectors.modbus.client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Mapping

from pymodbus import FramerType, ModbusException
from pymodbus.client import ModbusBaseSyncClient, ModbusSerialClient, ModbusTcpClient, ModbusUdpClient

import pandas as pd
import pytz as tz
from lories._core import ChannelState  # noqa
from lories.connectors import ConnectionError, Connector, ConnectorError, register_connector_type
from lories.connectors.modbus import ModbusRegister
from lories.core.configs import ConfigurationError
from lories.core.configs.parameters import ChannelParameter, Parameter, SelectParameter
from lories.typing import Configurations, Resources

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


@register_connector_type("modbus")
class ModbusClient(Connector):
    """
    Modbus is a widely used industrial communication protocol for connecting electronic devices over
    serial (RTU) or TCP/UDP networks. This connector uses the pymodbus library to read and write
    holding registers, input registers, and coils from Modbus slave devices. It supports configurable
    byte order, register grouping for multi-register data types (float32, int16, etc.), and automatic
    retry logic. However, Modbus lacks built-in authentication and encryption, and its register-based
    data model requires careful address mapping per device.
    """

    # Shared
    _protocol = SelectParameter(["tcp", "udp", "rtu"], key="protocol", desc="Modbus transport protocol")
    _endian = SelectParameter(["big", "little"], key="endian", default="big", desc="Byte order")
    _timeout = Parameter(key="timeout", type=pd.Timedelta, default="3s", min="1s", desc="Timeout")
    _retries = Parameter(key="retries", type=int, default=3, min=0, desc="Retry attempts")
    _scale = Parameter(key="scale", type=float, default=1.0, desc="Scale factor applied to all read values")
    # TCP / UDP
    _host = Parameter(key="host", type=str, required=False, desc="Remote host (tcp/udp)")
    _port = Parameter(key="port", type=int, required=False, default=502, min=1, max=65535, desc="Remote port (tcp/udp)")
    # Serial
    _com_port = Parameter(key="com_port", type=str, required=False, desc="Serial device path (e.g. /dev/ttyUSB0)")
    _baudrate = Parameter(key="baudrate", type=int, required=False, desc="Baud rate (serial)")
    _bytesize = Parameter(key="bytesize", type=int, required=False, default=8, desc="Byte size (serial)")
    _stopbits = Parameter(key="stopbits", type=int, required=False, default=1, desc="Stop bits (serial)")
    _parity = SelectParameter(["N", "E", "O"], key="parity", required=False, default="N", desc="Parity (serial)")

    # Per-channel parameters
    address = ChannelParameter(type=int, required=True, desc="Register start address (decimal or 0x hex)")
    function = ChannelParameter(
        type=str,
        required=False,
        default="holding_register",
        choices=["holding_register", "input_register", "coil"],
        desc="Modbus function code",
    )
    device = ChannelParameter(type=int, required=False, desc="Slave device ID (unit identifier)")
    data_type = ChannelParameter(type=str, required=False, desc="Override data type (e.g. float32, int16, string)")

    _protocol: str
    _endian: Literal["big", "little"]
    _timeout: pd.Timedelta
    _retries: int
    _scale: float
    _host: str
    _port: int
    _com_port: str
    _baudrate: int
    _bytesize: int
    _stopbits: int
    _parity: str

    __client: ModbusBaseSyncClient
    __registers: Mapping[str, ModbusRegister]

    # noinspection SpellCheckingInspection
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        if self._protocol == "tcp":
            self.__client = ModbusTcpClient(
                host=self._host,
                port=self._port,
                framer=FramerType.SOCKET,
                timeout=self._timeout.total_seconds(),
                retries=self._retries,
                # source_address=("localhost", 0),
            )
        elif self._protocol == "udp":
            self.__client = ModbusUdpClient(
                host=self._host,
                port=self._port,
                framer=FramerType.SOCKET,
                timeout=self._timeout.total_seconds(),
                retries=self._retries,
                # source_address=None,
            )
        elif self._protocol == "rtu":
            self.__client = ModbusSerialClient(
                port=self._com_port,
                framer=FramerType.RTU,
                timeout=self._timeout.total_seconds(),
                retries=self._retries,
                baudrate=self._baudrate,
                bytesize=self._bytesize,
                stopbits=self._stopbits,
                parity=self._parity,
                # handle_local_echo=False,
            )
        else:
            raise ConnectorError(self, f"Unknown modbus protocol type '{self._protocol}'")

    # noinspection PyUnresolvedReferences
    def is_connected(self) -> bool:
        return self.__client.connected

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        try:
            self._logger.info(f"Connecting to '{self.__client}'")
            self.__client.connect()
            self.__registers = {r.id: ModbusRegister.from_resource(r) for r in resources}

        except ModbusException as e:
            self._logger.warning(f"Error connecting to '{self.__client}': {e}")
            raise ConnectionError(self, e)
        except IOError as e:
            raise ConnectorError(self, e)

    def disconnect(self) -> None:
        super().disconnect()
        self.__client.close()

    # noinspection PyTypeChecker, PyShadowingBuiltins
    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz.UTC).floor(freq="s")
        data = pd.DataFrame(index=[timestamp], columns=resources.ids)
        try:
            for device, device_resources in resources.groupby("device"):
                if device is None:
                    device = 1

                # TODO: Implement reading adjacent blocks of registers of same device ID
                for resource in device_resources:
                    try:
                        register = self.__registers[resource.id]
                        function = getattr(self.__client, f"read_{register.function}s")

                        try:
                            result = function(register.address, count=register.length, device_id=device)
                        except TypeError:
                            try:
                                result = function(register.address, count=register.length, slave=device)
                            except TypeError:
                                result = function(register.address, count=register.length, unit=device)

                        if result.isError():
                            data.at[timestamp, resource.id] = ChannelState.UNKNOWN_ERROR
                            self._logger.warning(f"Error reading register '{resource.id}'")
                            continue

                        data.at[timestamp, resource.id] = self.__client.convert_from_registers(
                            result.registers, register.type, word_order=self._endian
                        )

                    except ConfigurationError as e:
                        data.at[timestamp, resource.id] = ChannelState.ARGUMENT_SYNTAX_ERROR
                        self._logger.warning(f"Invalid register configuration for resource '{resource.id}': {e}")
                        continue
                    except KeyError:
                        data.at[timestamp, resource.id] = ChannelState.NOT_AVAILABLE
                        continue
            return data

        except ModbusException as e:
            raise ConnectionError(self, e)
        except IOError as e:
            raise ConnectorError(self, e)

    def write(self, data: pd.DataFrame) -> None:
        try:
            for device, device_channels in self.channels.groupby("device"):
                if device is None:
                    device = 1

                for channel in device_channels:
                    if channel.id not in data.columns:
                        continue
                    channel_data = data.loc[:, channel.id].dropna(axis="index", how="all")
                    if channel_data.empty:
                        continue
                    register = self.__registers[channel.id]
                    try:
                        values = self.__client.convert_to_registers(
                            channel_data.iloc[-1], register.type, word_order=self._endian
                        )
                        self.__client.write_registers(register.address, values, slave=device)

                    except ConfigurationError as e:
                        self._logger.warning(f"Invalid register configuration for channel '{channel.id}': {e}")
                        continue

        except ModbusException as e:
            raise ConnectionError(self, e)
        except IOError as e:
            raise ConnectorError(self, e)
