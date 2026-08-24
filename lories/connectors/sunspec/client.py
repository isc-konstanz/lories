# -*- coding: utf-8 -*-
"""
lories.connectors.sunspec.client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from contextlib import nullcontext
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

from sunspec2.device import ModelError
from sunspec2.modbus.client import (
    SunSpecModbusClientDevice,
    SunSpecModbusClientDeviceRTU,
    SunSpecModbusClientDeviceTCP,
    SunSpecModbusClientError,
    SunSpecModbusClientTimeout,
    SunSpecModbusValueError,
)
from sunspec2.modbus.modbus import ModbusClientError

import pandas as pd
import pytz as tz
from lories._core import ChannelState  # noqa
from lories.connectors import ConnectionError, Connector, ConnectorError, register_connector_type
from lories.core.configs import ConfigurationError
from lories.core.configs.parameters import ChannelParameter, Parameter, SelectParameter
from lories.typing import Configurations, Resource, Resources

# One lock per serial port: pysunspec2 hands every RTU device on the same port the same
# unsynchronized serial client, and the framework's per-connector lock does not span the
# connector instances sharing that port.
_serial_locks: Dict[str, Lock] = {}
_serial_locks_guard = Lock()


def _serial_port_lock(com_port: str) -> Lock:
    with _serial_locks_guard:
        return _serial_locks.setdefault(com_port, Lock())


@register_connector_type("sunspec")
class SunSpecClient(Connector):
    """
    SunSpec is an open interoperability standard of the SunSpec Alliance for distributed energy
    resources: inverters, meters, storage, and environmental sensors expose self-describing data
    models over Modbus TCP or RTU. This connector uses the pysunspec2 reference library to scan
    the device's well-known base addresses for the 'SunS' marker, walk the model chain, and bind
    channels to model points by name instead of raw register addresses. Scale-factor registers
    are applied automatically and not-implemented points are reported as unavailable. Channels
    address a value with `model` (numeric id or group name), `point` (name, or a dotted path
    with 1-based indices into repeating groups, e.g. "module.2.DCW"), and optionally `instance`
    when a model occurs repeatedly on the device (e.g. multiple meters). Writing a channel
    writes the corresponding point, which covers the standard control models.
    """

    # Shared
    _protocol = SelectParameter(
        ["tcp", "rtu"],
        key="protocol",
        required=True,
        desc="Modbus transport protocol (selects tcp/rtu branch)",
    )
    _device_id = Parameter(
        key="device_id",
        type=int,
        default=1,
        min=1,
        max=247,
        desc="Modbus unit identifier / slave id of the SunSpec device",
    )
    _timeout = Parameter(key="timeout", type=pd.Timedelta, default="3s", min="1s", desc="Modbus request timeout")
    # TCP
    _host = Parameter(key="host", type=str, required=False, desc="Remote device hostname or IP (used by tcp protocol)")
    _port = Parameter(
        key="port",
        type=int,
        required=False,
        default=502,
        min=1,
        max=65535,
        desc="Remote device TCP port (SolarEdge SetApp-configured units default to 1502)",
    )
    # Serial
    _com_port = Parameter(
        key="com_port",
        type=str,
        required=False,
        desc="Serial device path (e.g. /dev/ttyUSB0; used by rtu protocol)",
    )
    _baudrate = Parameter(
        key="baudrate", type=int, required=False, default=9600, min=1, desc="Serial baud rate (used by rtu protocol)"
    )
    _parity = SelectParameter(
        ["N", "E"],
        key="parity",
        required=False,
        default="N",
        desc="Parity setting: N=none, E=even (odd parity is not supported by pysunspec2)",
    )

    # Per-channel parameters
    model = ChannelParameter(
        type=str,
        required=True,
        desc="SunSpec model: numeric id (e.g. 103) or group name (e.g. 'inverter_three_phase')",
    )
    point = ChannelParameter(
        type=str,
        required=True,
        desc="Point name within the model; dotted path with 1-based indices for repeating groups (e.g. 'module.2.DCW')",
    )
    instance = ChannelParameter(
        type=int,
        required=False,
        default=1,
        desc="1-based instance if the model occurs repeatedly on the device (e.g. multiple meters)",
    )

    _protocol: str
    _device_id: int
    _timeout: pd.Timedelta
    _host: str
    _port: int
    _com_port: str
    _baudrate: int
    _parity: str

    __device: Optional[SunSpecModbusClientDevice] = None
    __healthy: bool = False
    __points: Dict[str, Any]
    __models: Dict[str, Any]

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)

        if self._protocol == "tcp":
            if not self._host:
                raise ConfigurationError("SunSpec tcp protocol requires a 'host' address")
        elif self._protocol == "rtu":
            if not self._com_port:
                raise ConfigurationError("SunSpec rtu protocol requires a 'com_port' device path")
        else:
            raise ConnectorError(self, f"Unknown sunspec protocol type '{self._protocol}'")

    def is_connected(self) -> bool:
        # The RTU device's own is_connected() is a hardcoded True stub, so a local health
        # flag (cleared on transport errors) must drive the framework's reconnect gate
        return self.__device is not None and self.__healthy and self.__device.is_connected()

    def __transport_lock(self):
        if self._protocol == "rtu":
            return _serial_port_lock(self._com_port)
        return nullcontext()

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        try:
            with self.__transport_lock():
                if self._protocol == "tcp":
                    self._logger.info(f"Connecting to SunSpec device {self._host}:{self._port}#{self._device_id}")
                    self.__device = SunSpecModbusClientDeviceTCP(
                        slave_id=self._device_id,
                        ipaddr=self._host,
                        ipport=self._port,
                        timeout=self._timeout.total_seconds(),
                    )
                else:
                    self._logger.info(f"Connecting to SunSpec device {self._com_port}#{self._device_id}")
                    # The RTU device opens the (shared) serial port on construction; its connect() is a no-op
                    self.__device = SunSpecModbusClientDeviceRTU(
                        slave_id=self._device_id,
                        name=self._com_port,
                        baudrate=self._baudrate,
                        parity=self._parity,
                        timeout=self._timeout.total_seconds(),
                    )
                self.__device.connect()
                # scan() would disconnect afterward if it opened the connection itself
                self.__device.scan(connect=False, full_model_read=True)

        except (SunSpecModbusClientError, ModbusClientError) as e:
            # A failed connect never reaches disconnect(), so release the transport here
            self.__close()
            raise ConnectionError(self, e)
        except IOError as e:
            self.__close()
            raise ConnectorError(self, e)
        self.__healthy = True

        models = sorted(mid for mid in self.__device.models.keys() if isinstance(mid, int))
        self._logger.info(f"Discovered SunSpec models: {models}")
        common = self.__device.models.get("common")
        if common:
            info = {key: common[0].points[key].cvalue for key in ("Mn", "Md", "SN", "Vr") if key in common[0].points}
            info = {key: value for key, value in info.items() if value}
            if info:
                self._logger.info("SunSpec device identity: " + ", ".join(f"{k}={v}" for k, v in info.items()))

        self.__points = {}
        self.__models = {}
        for resource in resources:
            try:
                model, point = self._resolve_point(resource)
                self.__models[resource.id] = model
                self.__points[resource.id] = point
            except ConfigurationError as e:
                self._logger.warning(f"Invalid SunSpec point configuration for resource '{resource.id}': {e}")

    def disconnect(self) -> None:
        super().disconnect()
        self.__close()

    def __close(self) -> None:
        self.__healthy = False
        if self.__device is None:
            return
        try:
            with self.__transport_lock():
                if self._protocol == "rtu":
                    # Deregisters from the shared serial client, closing the port with its last device
                    self.__device.close()
                else:
                    self.__device.disconnect()
        except (SunSpecModbusClientError, ModbusClientError, IOError) as e:
            self._logger.warning(f"Error closing SunSpec device: {e}")
        finally:
            self.__device = None

    # noinspection PyShadowingBuiltins
    def _resolve_point(self, resource: Resource) -> Tuple[Any, Any]:
        model_key = resource.get("model")
        key = str(model_key).strip()
        if key.isdigit():
            key = int(key)
        models = self.__device.models
        if key not in models:
            discovered = sorted(mid for mid in models.keys() if isinstance(mid, int))
            raise ConfigurationError(f"SunSpec model '{model_key}' not present on device (discovered: {discovered})")
        instances = models[key]
        try:
            instance = int(resource.get("instance", default=1))
        except (TypeError, ValueError):
            raise ConfigurationError(f"Invalid SunSpec model instance '{resource.get('instance')}'")
        if not 1 <= instance <= len(instances):
            raise ConfigurationError(
                f"SunSpec model '{model_key}' instance {instance} out of range ({len(instances)} present)"
            )
        model = instances[instance - 1]

        path = str(resource.get("point")).strip()
        group = model
        segments = path.split(".")
        while len(segments) > 1:
            name = segments.pop(0)
            subgroup = group.groups.get(name)
            if subgroup is None:
                raise ConfigurationError(f"SunSpec group '{name}' not found in point path '{path}'")
            if isinstance(subgroup, list):
                if not segments or not segments[0].isdigit():
                    raise ConfigurationError(
                        f"Repeating SunSpec group '{name}' requires a 1-based index in point path '{path}'"
                    )
                index = int(segments.pop(0))
                if not 1 <= index <= len(subgroup):
                    raise ConfigurationError(
                        f"Index {index} out of range for repeating SunSpec group '{name}' "
                        f"({len(subgroup)} instances) in point path '{path}'"
                    )
                subgroup = subgroup[index - 1]
            group = subgroup
        point = group.points.get(segments[0])
        if point is None:
            raise ConfigurationError(f"SunSpec point '{path}' not found in model '{model_key}'")
        return model, point

    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz.UTC).floor(freq="s")
        data = pd.DataFrame(index=[timestamp], columns=resources.ids)
        try:
            blocks: Dict[int, Tuple[Any, List[Resource]]] = {}
            for resource in resources:
                model = self.__models.get(resource.id)
                if model is None:
                    data.at[timestamp, resource.id] = ChannelState.NOT_AVAILABLE
                    continue
                blocks.setdefault(id(model), (model, []))[1].append(resource)

            for model, block_resources in blocks.values():
                try:
                    # One block read per model keeps values and their scale factors consistent
                    with self.__transport_lock():
                        model.read()
                except (SunSpecModbusClientTimeout, ModbusClientError):
                    raise
                except SunSpecModbusClientError as e:
                    self._logger.warning(f"Error reading SunSpec model block: {e}")
                    for resource in block_resources:
                        data.at[timestamp, resource.id] = ChannelState.UNKNOWN_ERROR
                    continue

                for resource in block_resources:
                    try:
                        value = self.__points[resource.id].cvalue
                    except ModelError as e:
                        self._logger.warning(f"Error resolving SunSpec value for '{resource.id}': {e}")
                        data.at[timestamp, resource.id] = ChannelState.UNKNOWN_ERROR
                        continue
                    if value is None:
                        data.at[timestamp, resource.id] = ChannelState.NOT_AVAILABLE
                        continue
                    if isinstance(value, str):
                        value = value.rstrip("\x00").strip()
                    data.at[timestamp, resource.id] = value
            return data

        except (SunSpecModbusClientTimeout, ModbusClientError) as e:
            self.__healthy = False
            raise ConnectionError(self, e)
        except SunSpecModbusClientError as e:
            raise ConnectorError(self, e)
        except IOError as e:
            raise ConnectorError(self, e)

    def write(self, data: pd.DataFrame) -> None:
        try:
            for channel in self.channels:
                if channel.id not in data.columns:
                    continue
                channel_data = data.loc[:, channel.id].dropna(axis="index", how="all")
                if channel_data.empty:
                    continue
                point = self.__points.get(channel.id)
                if point is None:
                    self._logger.warning(f"Cannot write unresolved SunSpec channel '{channel.id}'")
                    continue
                value = channel_data.iloc[-1]
                if hasattr(value, "item"):
                    # Unwrap numpy scalars, which pysunspec2's register packing rejects
                    value = value.item()
                try:
                    point.cvalue = value
                    with self.__transport_lock():
                        point.write()

                except (SunSpecModbusValueError, ModelError, TypeError, ValueError) as e:
                    self._logger.warning(f"Invalid value writing SunSpec channel '{channel.id}': {e}")
                    continue

        except (SunSpecModbusClientTimeout, ModbusClientError) as e:
            self.__healthy = False
            raise ConnectionError(self, e)
        except SunSpecModbusClientError as e:
            raise ConnectorError(self, e)
        except IOError as e:
            raise ConnectorError(self, e)
