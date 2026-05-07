# -*- coding: utf-8 -*-
"""
lories.connectors.serial.sdi12
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import time
from threading import Event, Thread
from typing import AnyStr, Dict, Optional

import pandas as pd
import pytz as tz
from lories.connectors import ConnectionError, register_connector_type
from lories.connectors.serial._core import _SerialConnector
from lories.core.configs.errors import ConfigurationError
from lories.data import ChannelState
from lories.typing import Resources
from lories.util import is_int, to_timedelta


@register_connector_type("sdi12")
class Sdi12Connector(_SerialConnector):
    """
    SDI-12 connector.

    SDI-12 (Serial Data Interface at 1200 baud) is a half-duplex, single-master
    serial protocol used by environmental sensors — soil moisture, water level,
    weather, etc. Up to ~10 sensors share one twisted-pair bus. Each sensor has
    a one-character address (typically '0'-'9'). A standard measurement is a
    three-step exchange:

        master → ``aM!``                    request a measurement from sensor ``a``
        sensor → ``atttn<CR><LF>``          ttt = seconds until ready, n = #values
        ...wait ttt seconds...
        master → ``aD0!`` [, ``aD1!`` ...]  fetch each data frame in order
        sensor → ``a±vv.v±vv.v...<CR><LF>`` packed sign-prefixed values

    Sensors sleep after ~100 ms of bus idle and require an SDI-12 *break*
    (continuous spacing for ≥12 ms followed by ≥8.33 ms marking) before the
    next command to wake them — this connector issues a break before every
    command on the bus.

    A daemon thread performs the full M!/D! cycle for every configured sensor
    every ``freq`` seconds and caches the latest values. ``read()`` returns
    the cached values stamped with the current time, so per-channel reads do
    not block on the serial bus.

    Channels declare which sensor address they live on, which D-frame to read,
    and the index of their value inside that frame:

    .. code-block:: toml

        [connectors.mySdi12Connector]
        type     = "sdi12"
        port     = "/dev/ttyUSB0"
        baudrate = 1200
        freq     = "30s"

        [data.channels.soil_temp]
        connector = "soil"
        sensor    = 0          # SDI-12 address
        data      = 0          # D0! frame
        index     = 0          # first value in the frame
    """

    _freq_seconds: float = 60.0
    _last_values: Optional[Dict[str, float]] = None
    _measurement_thread: Optional[Thread] = None
    _stop_event: Optional[Event] = None

    def configure(self, configs) -> None:
        super().configure(configs)
        freq = configs.get("freq", default="60s")
        self._freq_seconds = to_timedelta(freq).total_seconds()

    def _on_connect(self, resources: Resources) -> None:
        self._last_values = None
        self._stop_event = Event()
        self._measurement_thread = Thread(
            target=self._measurement_loop,
            args=(resources,),
            daemon=True,
            name=f"sdi12-{self.id}",
        )
        self._measurement_thread.start()

    def _at_disconnect(self) -> None:
        if self._stop_event is not None:
            self._stop_event.set()
        if self._measurement_thread is not None:
            self._measurement_thread.join(timeout=30)
            self._measurement_thread = None

    def _measurement_loop(self, resources: Resources) -> None:
        while not self._stop_event.is_set():
            if len(resources) > 0:
                try:
                    self._run_measurement(resources)
                except ConnectionError as e:
                    self._logger.error(f"SDI12 connection lost, will reconnect: {e}")
                    try:
                        self._serial.close()
                    except Exception:
                        pass
                    return
                except Exception:
                    self._logger.exception("SDI12 background measurement failed")
            self._stop_event.wait(self._freq_seconds)

    def _run_measurement(self, resources: Resources) -> None:
        """Perform a full SDI12 measurement cycle sequentially and cache the result."""
        results = {}

        for sensor_address, sensor_resources in resources.groupby("sensor"):
            if not is_int(sensor_address):
                self._logger.warning(f"Invalid SDI12 sensor address: {sensor_address}")
                continue

            ttt = self._start_measurement(str(sensor_address))
            if ttt is None:
                continue

            if ttt > 0 and self._stop_event.wait(ttt):
                return  # shutdown requested during measurement wait

            try:
                sensor_data = self._collect_data(sensor_resources, str(sensor_address))
                if sensor_data is not None:
                    results.update(sensor_data)
            except IOError as e:
                raise ConnectionError(self, f"Failed to read from SDI12 sensor at address {sensor_address}: {e}")

        if results:
            self._last_values = results

    def read(self, resources: Resources) -> pd.DataFrame:
        if self._last_values is None:
            return pd.DataFrame()
        return pd.DataFrame(index=[pd.Timestamp.now(tz.UTC).floor("s")], data=self._last_values)

    def _break(self) -> None:
        """SDI-12 break: ≥12 ms spacing + ≥8.33 ms marking before next command."""
        self._serial.break_condition = True
        time.sleep(0.013)
        self._serial.break_condition = False
        time.sleep(0.0085)

    def _start_measurement(self, address: AnyStr) -> Optional[int]:
        """Send aM! and return the ttt (seconds to wait), or None on failure."""
        self._break()
        self._serial.reset_input_buffer()
        self._write_string(f"{address}M!\r\n")
        response = self._read_line()
        if not response.startswith(address):
            self._logger.warning(f"Invalid SDI12 response to {address}M!: {response!r}")
            return None
        try:
            return int(response[len(address) : len(address) + 3])
        except ValueError:
            self._logger.warning(f"Invalid SDI12 response to {address}M!: {response!r}")
            return None

    def _collect_data(
        self,
        resources: Resources,
        address: AnyStr,
    ) -> Optional[Dict[str, float]]:
        """Send aD0! … aDn! sequentially and parse values.

        D! frames must be queried in order starting from D0!, even if no channels
        are mapped to lower frames — some sensors only return data for Di! after
        D(i-1)! has been read.
        """
        data_groups = {int(data): data_resources for data, data_resources in resources.groupby("data") if is_int(data)}
        if not data_groups:
            return None

        max_data = max(data_groups)
        results = {}

        for data_index in range(max_data + 1):
            self._break()
            self._serial.reset_input_buffer()
            self._write_string(f"{address}D{data_index}!\r\n")

            response = self._read_line()
            if response == address:
                # Service request (address-only frame) arrived before the data response — read again
                response = self._read_line()
            if not response or not response.startswith(address):
                self._logger.warning(f"Invalid SDI12 response to D{data_index}!: {response!r}")
                break

            if data_index not in data_groups:
                # No channels mapped to this frame — queried only to advance the sensor pointer
                continue

            self._logger.debug(f"SDI12 [{address}] D{data_index}! raw: {response!r}")
            response_parts = response[1:].replace("+", " +").replace("-", " -").split()

            for index, index_resources in data_groups[data_index].groupby("index"):
                if index is None or not is_int(index):
                    raise ConfigurationError(
                        f"SDI12 channel(s) {[r.id for r in index_resources]} on sensor {address} "
                        f"D{data_index}! have invalid index: {index!r}"
                    )
                index = int(index)

                for resource in index_resources:
                    try:
                        value = float(response_parts[index])
                        results[resource.id] = value
                        self._logger.debug(f"SDI12 [{address}] D{data_index}![{index}] → {resource.id} = {value}")
                    except (IndexError, ValueError):
                        results[resource.id] = ChannelState.READ_ERROR
                        self._logger.warning(
                            f"Failed to parse SDI12 value for sensor {address} from response: {response!r}"
                        )

        if len(results) == 0:
            return None
        return results

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("SDI12 does not support writing data")
