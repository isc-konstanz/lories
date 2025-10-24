# -*- coding: utf-8 -*-
"""
lories.connectors.serial.sdi12
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import time
from typing import AnyStr, Dict, Optional

import pandas as pd
import pytz as tz
from lories.connectors import ConnectionError, register_connector_type
from lories.connectors.serial._serial import _SerialConnector
from lories.data import ChannelState
from lories.typing import Resources


@register_connector_type("sdi12")
class Sdi12Connector(_SerialConnector):
    def read(self, resources: Resources) -> pd.DataFrame:
        """
        Read all sensors.
        Performs: break → aM! → wait → aD0! … Dn!
        """
        timestamp = pd.Timestamp.now(tz=tz.UTC).floor(freq="s")

        results = {}
        for sensor_address, sensor_resources in resources.groupby("sensor"):
            sensor_data = self._read_sensor(sensor_resources, sensor_address)
            if sensor_data is not None:
                results.update(sensor_data)

        if len(results) == 0:
            return pd.DataFrame()
        return pd.DataFrame(index=[timestamp], data=results)

    def _read_sensor(
        self,
        resources: Resources,
        address: AnyStr,
    ) -> Optional[Dict[str, float]]:
        """
        Perform a full measurement cycle:
        aM! → parse tttn → wait/service → aD0! … Dn!
        """
        self._break()
        self._write_string(f"{address}M!\r\n")
        response = self._read_line()
        if not response.startswith(address):
            raise ConnectionError(f"Invalid response to M!: {response}")

        # Extract the time to wait in seconds
        ttt = int(response[1:4])
        if ttt > 0:
            time.sleep(ttt)

        results = {}
        for data, data_resources in resources.groupby("data"):
            self._break()
            self._write_string(f"{address}D{data}!\r\n")

            response = self._read_line()
            if not response or not response.startswith(address):
                self._logger.warning(f"Invalid SDI12 response to D{data}!: {response}")
                break

            response_parts = response[1:].replace("+", " +").replace("-", " -").split()
            for index, index_resources in data_resources.groupby("index"):
                if index is None:
                    index = 0
                for resource in index_resources:
                    try:
                        results[resource.id] = float(response_parts[index])

                    except (IndexError, ValueError):
                        results[resource.id] = ChannelState.READ_ERROR
                        self._logger.warning(
                            f"Failed to parse SDI12 value for sensor {address} from response: {response}"
                        )
        if len(results) > 0:
            return results
        return None

    def _break(self) -> None:
        """Issue SDI-12 break (≥12 ms of spacing, i.e. logic 0)."""
        self._serial.break_condition = True
        time.sleep(0.015)
        self._serial.break_condition = False

        # ≥8.33 ms mark before next command
        time.sleep(0.0085)

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("SDI12 does not support writing data")
