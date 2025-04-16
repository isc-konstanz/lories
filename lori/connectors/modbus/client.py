# -*- coding: utf-8 -*-
"""
lori.connectors.modbus.client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Mapping

from pymodbus import FramerType, ModbusException
from pymodbus.client import ModbusBaseSyncClient, ModbusSerialClient, ModbusTcpClient, ModbusUdpClient

import pandas as pd
import pytz as tz
from lori.connectors import ConnectionException, Connector, ConnectorException, register_connector_type
from lori.connectors.modbus import ModbusRegister
from lori.core import ConfigurationException, Configurations, Resources
from lori.data import ChannelState

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import Literal

except ImportError:
    from typing_extensions import Literal


@register_connector_type("modbus")
class ModbusClient(Connector):
    __client: ModbusBaseSyncClient
    __registers: Mapping[str, ModbusRegister]

    _endian: Literal["big", "little"]

    # noinspection SpellCheckingInspection
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        _endian = configs.get("word_order", default="big").lower()
        if _endian not in ["big", "little"]:
            raise ConnectorException(self, f"Invalid modbus word order '{_endian}'")
        self._endian = _endian

        timeout = configs.get_int("timeout", default=3)
        retries = configs.get_int("retries", default=3)

        protocol = configs.get("protocol").lower()
        if protocol == "tcp":
            self.__client = ModbusTcpClient(
                host=configs.get("host"),
                port=configs.get_int("port", 502),
                framer=FramerType.SOCKET,
                timeout=timeout,
                retries=retries,
                # source_address=("localhost", 0),
            )
        elif protocol == "udp":
            self.__client = ModbusUdpClient(
                host=configs.get("host"),
                port=configs.get_int("port", 502),
                framer=FramerType.SOCKET,
                timeout=timeout,
                retries=retries,
                # source_address=None,
            )
        elif protocol in ["rtu", "serial"]:
            self.__client = ModbusSerialClient(
                port=configs.get("port"),
                framer=FramerType.RTU,
                timeout=timeout,
                retries=retries,
                baudrate=configs.get_int("baudrate"),
                bytesize=configs.get_int("bytesize", default=8),
                stopbits=configs.get_int("stopbits", default=1),
                parity=configs.get("parity", default="N"),
                # handle_local_echo=False,
            )
        else:
            raise ConnectorException(self, f"Unknown modbus protocol type '{protocol}'")

    # noinspection PyUnresolvedReferences
    def is_connected(self) -> bool:
        return self.__client.connected

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        try:
            self._logger.info(f"Connecting to '{self.__client}'")
            self.__client.connect()
            self.__registers = {r.id: ModbusRegister.from_resource(r) for r in resources}

            self._logger.info([f"{r}: {reg.address}" for r, reg in self.__registers.items()])

        except ModbusException as e:
            self._logger.warning(f"Error connecting to '{self.__client}': {e}")
            raise ConnectionException(self, e)
        except IOError as e:
            raise ConnectorException(self, e)

    def disconnect(self) -> None:
        super().disconnect()
        self.__client.close()

    # noinspection PyTypeChecker, PyShadowingBuiltins
    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz.UTC).floor(freq="s")
        data = pd.DataFrame(index=[timestamp], columns=[resources.ids])
        try:
            for device, device_resources in resources.groupby("device"):
                if device is None:
                    device = 1

                # TODO: Implement reading adjacent blocks of registers of same device ID
                for resource in device_resources:
                    register = self.__registers[resource.id]
                    try:
                        function = getattr(self.__client, f"read_{register.function}")

                        self._logger.info(f"{function}({register.address}, count={register.length}, slave={device})")

                        result = function(register.address, count=register.length, slave=device)
                        if result.isError():
                            data.at[timestamp, resource.id] = ChannelState("REGISTER_READ_ERROR")
                            self._logger.warning(f"Error reading register '{resource.id}'")
                            continue

                        self._logger.info(f"convert_from_registers({result.registers}, {register.type}, word_order={self._endian})")

                        value = self.__client.convert_from_registers(
                            result.registers,
                            register.type,
                            word_order=self._endian
                        )
                        data.at[timestamp, resource.id] = value

                    except ConfigurationException as e:
                        data.at[timestamp, resource.id] = ChannelState.ARGUMENT_SYNTAX_ERROR
                        self._logger.warning(f"Invalid register configuration for resource '{resource.id}': {e}")
                        continue

        except ModbusException as e:
            raise ConnectionException(self, e)
        except IOError as e:
            raise ConnectorException(self, e)

        if len(data) == 0:
            return pd.DataFrame()
        return pd.concat(data, axis="columns")

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
                            channel_data.iloc[-1],
                            register.type,
                            word_order=self._endian
                        )
                        self.__client.write_registers(register.address, values, slave=device)

                    except ConfigurationException as e:
                        self._logger.warning(f"Invalid register configuration for channel '{channel.id}': {e}")
                        continue

        except ModbusException as e:
            raise ConnectionException(self, e)
        except IOError as e:
            raise ConnectorException(self, e)
