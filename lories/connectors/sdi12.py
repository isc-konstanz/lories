# -*- coding: utf-8 -*-
"""
lories.connectors.sdi12
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import time
import serial
import pandas as pd
import pytz as tz
from typing import Dict

from lories.data import Channel, Channels
from lories.core import Configurations, Resources
from lories.connectors import register_connector_type
from lories.connectors.serial import SerialConnector


@register_connector_type("sdi12")
class Sdi12Connector(SerialConnector):
    """
    SDI-12 connector for communicating with sensors over a serial bus.
    """

    _sensors: Dict[str, Sdi12Sensor]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sensors = {}

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)


    def connect(self, resources: Resources) -> None:
        """Open the serial port and initialize sensor listeners."""
        super().connect(resources)

        channels = resources.filter(lambda r: isinstance(r, Channel))
        for sensor_addr, grouped_channels in channels.groupby("sensor"):
            sensor = Sdi12Sensor(sensor_addr, self._serial)
            for channel in grouped_channels:
                sensor.add_channel(channel)
            self._sensors[sensor_addr] = sensor

        self._logger.info(f"Connected SDI-12 bus on {self.port} at {self.baudrate} baud")

    def disconnect(self) -> None:
        super().disconnect()

    def is_connected(self) -> bool:
        return super().is_connected()

    def read(self, resources: Resources) -> pd.DataFrame:
        """
        Read all sensors.
        Performs: break → aM! → wait → aD0! … Dn!
        """
        now = pd.Timestamp.now(tz=tz.UTC)
        data = []
        for sensor_addr, sensor in self._sensors.items():
            measurements = sensor.read_measurements()
            for channel_id, value in measurements.items():
                data.append({
                    "timestamp": now,
                    "channel": channel_id,
                    "value": value
                })

        df = pd.DataFrame(data)
        return df
        
                


class Sdi12Sensor:
    """
    Represents an SDI-12 sensor at a specific address.
    Handles standard commands: a!, aI!, aM!, aD0!, etc.
    """

    _channels: Channels

    def __init__(self, address: str, serial_port: serial.Serial):
        self.address = address
        self.serial = serial_port
        self.channels = Channels()

    def add_channel(self, channel: Channel) -> None:
        self.channels.append(channel)

    def _send(self, command: str) -> None:
        """Send command to the sensor."""
        self.serial.write(command.encode("ascii"))

    def _readline(self) -> str:
        """Read one line (terminated by CR/LF)."""
        line = self.serial.readline().decode("ascii", errors="ignore").strip()
        return line

    def _break(self) -> None:
        """Issue SDI-12 break (≥12 ms of spacing, i.e. logic 0)."""
        self.serial.break_condition = True
        time.sleep(0.015)
        self.serial.break_condition = False
        time.sleep(0.0085)  # ≥8.33 ms mark before next command

    def read_measurements(self) -> Dict[str, float]:
        """
        Perform a full measurement cycle:
        aM! → parse tttn → wait/service → aD0! … Dn!
        Returns dict of {channel_id: value}.
        """
        self._break()
        self._send(f"{self.address}M!\r\n")
        resp = self._readline()
        if not resp.startswith(self.address):
            raise IOError(f"Invalid response to M!: {resp}")

        ttt = int(resp[1:4])  # time to wait in seconds
        if ttt > 0:
            time.sleep(ttt)

        results = {}
        for data, data_channels in self.channels.groupby("data"):
            self._break()
            self._send(f"{self.address}D{data}!\r\n")
            r = self._readline()
            if not r or not r.startswith(self.address):
                #self._logger.error(f"Invalid response to D{data}!: {r}")
                break
            
            part = r[1:].replace("+", " +").replace("-", " -").split()
            for index, grouped_channels in data_channels.groupby("data_index"):
                if index is None:
                    index = 0
                    
                for ch in grouped_channels:
                    try:
                        value = float(part[index])
                        results[ch.id] = value
                    except (IndexError, ValueError):
                        #self.logger.error(f"Failed to parse value for channel {ch.id} from response: {r}")
                        pass
                        results[ch.id] = None
                
        return results
