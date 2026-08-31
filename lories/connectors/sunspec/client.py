# -*- coding: utf-8 -*-
"""
lories.connectors.sunspec.client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from sunspec2.device import ModelError
from sunspec2.modbus.client import (
    SunSpecModbusClientDevice,
    SunSpecModbusClientDeviceRTU,
    SunSpecModbusClientDeviceTCP,
    SunSpecModbusClientError,
    SunSpecModbusValueError,
)
from sunspec2.modbus.modbus import (
    ModbusClientError,
    ModbusClientTimeout,
    modbus_rtu_client,
    modbus_rtu_client_remove,
)

import pandas as pd
import pytz as tz
from lories._core import ChannelState  # noqa
from lories.connectors import ConnectionError, Connector, ConnectorError, register_connector_type
from lories.core.configs import ConfigurationError
from lories.core.configs.parameters import ChannelParameter, Parameter, SelectParameter
from lories.typing import Configurations, Resource, Resources


@register_connector_type("sunspec")
class SunSpecClient(Connector):
    """
    SunSpec is an open interoperability standard of the SunSpec Alliance for distributed energy
    resources: inverters, meters, storage, and environmental sensors expose self-describing data
    models over Modbus TCP or RTU. This connector uses the pysunspec2 reference library to scan
    the device's well-known base addresses for the 'SunS' marker, walk the model chain, and bind
    channels to model points by name instead of raw register addresses. Scale-factor registers
    are applied automatically and not-implemented points are reported as unavailable.

    One connector owns one transport endpoint: a serial port for `rtu`, a host and port for
    `tcp`. Several SunSpec devices behind that endpoint are addressed by the per-channel
    `device` key, the Modbus unit identifier, exactly as the `modbus` connector groups channels
    by its own `device` key. A channel further addresses a value with `model` (numeric id or
    group name), `point` (name, or a dotted path with 1-based indices into repeating groups,
    e.g. "module.2.DCW"), and optionally `instance` when a model occurs repeatedly on that one
    device (e.g. multiple meters). Writing a channel writes the corresponding point, which
    covers the standard control models.

    Units are scanned lazily on their first read rather than at connect, and a unit that stops
    answering is benched for the reconnect interval without disturbing its siblings on the same
    transport, so an inverter that sleeps overnight cannot stop a meter beside it from logging.
    """

    # Shared
    _protocol = SelectParameter(
        ["tcp", "rtu"],
        key="protocol",
        required=True,
        desc="Modbus transport protocol (selects tcp/rtu branch)",
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
    device = ChannelParameter(
        type=int,
        required=True,
        desc="Modbus unit identifier / slave id of the SunSpec device this channel reads from",
    )
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
    _timeout: pd.Timedelta
    _host: str
    _port: int
    _com_port: str
    _baudrate: int
    _parity: str

    # The shared pysunspec2 serial client for _com_port, rtu only. Every RTU device on a port
    # gets handed this same object, and pysunspec2 requires those requests to be single
    # threaded, which the connector's own lock provides now that one connector owns the port.
    __client: Optional[Any] = None
    __healthy: bool = False
    __devices: Optional[Dict[int, SunSpecModbusClientDevice]] = None
    __resolved: Optional[Dict[int, Dict[str, Optional[Tuple[Any, Any]]]]] = None
    __unavailable: Optional[Dict[int, pd.Timestamp]] = None

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
        # Transport health only. A single unit that stops answering is benched, not reported
        # here, and the RTU device's own is_connected() is a hardcoded True stub, so a local
        # flag cleared on transport errors must drive the framework's reconnect gate
        return self.__healthy

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        self.__devices = {}
        self.__resolved = {}
        self.__unavailable = {}
        try:
            if self._protocol == "rtu":
                self._logger.info(f"Opening SunSpec serial transport {self._com_port}")
                # Opens the port. pysunspec2 keys these clients by port name process-wide and
                # rejects a mismatched baudrate or parity, so the transport settings stay
                # connector-level while the unit id lives on the channel.
                self.__client = modbus_rtu_client(
                    name=self._com_port,
                    baudrate=self._baudrate,
                    parity=self._parity,
                    timeout=self._timeout.total_seconds(),
                )
            else:
                self._logger.info(f"Using SunSpec gateway {self._host}:{self._port}")

        except (SunSpecModbusClientError, ModbusClientError) as e:
            self.__close()
            raise ConnectionError(self, e)
        except IOError as e:
            self.__close()
            raise ConnectorError(self, e)
        self.__healthy = True

        units = sorted(u for u in {self._resource_device(r) for r in resources} if u is not None)
        # Scanning is deferred to the first read of each unit, so a device that is asleep at
        # startup cannot keep the transport, or its siblings, from coming up
        self._logger.info(f"SunSpec transport connected, units to scan on first read: {units}")

    def disconnect(self) -> None:
        super().disconnect()
        self.__close()

    def __close(self) -> None:
        self.__healthy = False
        for device_id in list((self.__devices or {}).keys()):
            self.__close_device(self.__devices.pop(device_id))
        if self.__resolved is not None:
            self.__resolved.clear()
        if self.__unavailable is not None:
            self.__unavailable.clear()

        if self.__client is not None:
            try:
                # Closing every device already dropped the port's last reference, but a
                # connector that never scanned a unit still holds the client it opened
                self.__client.close()
                modbus_rtu_client_remove(self._com_port)
            except (SunSpecModbusClientError, ModbusClientError, IOError) as e:
                self._logger.warning(f"Error closing SunSpec serial transport: {e}")
            finally:
                self.__client = None

    def __close_device(self, device: SunSpecModbusClientDevice) -> None:
        try:
            if self._protocol == "rtu":
                # Deregisters from the shared serial client, closing the port with its last device
                device.close()
            else:
                device.disconnect()
        except (SunSpecModbusClientError, ModbusClientError, IOError) as e:
            self._logger.warning(f"Error closing SunSpec device: {e}")

    # ------------------------------------------------------------------ units

    # noinspection PyMethodMayBeStatic
    def _resource_device(self, resource: Resource) -> Optional[int]:
        """Return the Modbus unit id of a resource, or None when it is missing or unusable."""
        value = resource.get("device")
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def __device(self, device_id: int) -> Optional[SunSpecModbusClientDevice]:
        """Return the scanned device for a unit id, scanning it if it is due, else None."""
        device = self.__devices.get(device_id)
        if device is not None:
            return device

        failed_at = self.__unavailable.get(device_id)
        if failed_at is not None and failed_at + self._interval_reconnect > pd.Timestamp.now(tz.UTC):
            # Benched: spend no bus time on a unit that is not answering, so its siblings
            # keep the transport to themselves
            return None

        try:
            device = self.__scan(device_id)
        except SunSpecModbusClientError as e:
            # scan() swallows the underlying timeout and reports a failed base-address probe,
            # so a silent unit and a non-SunSpec unit both land here. Neither is a reason to
            # drop the transport.
            self._logger.warning(
                f"SunSpec device {device_id} did not scan, retrying in {self._interval_reconnect}: {e}"
            )
            self.__bench(device_id)
            return None
        except ModbusClientTimeout as e:
            self._logger.warning(
                f"SunSpec device {device_id} timed out scanning, retrying in {self._interval_reconnect}: {e}"
            )
            self.__bench(device_id)
            return None

        self.__unavailable.pop(device_id, None)
        self.__devices[device_id] = device
        return device

    def __scan(self, device_id: int) -> SunSpecModbusClientDevice:
        if self._protocol == "tcp":
            self._logger.info(f"Scanning SunSpec device {self._host}:{self._port}#{device_id}")
            device = SunSpecModbusClientDeviceTCP(
                slave_id=device_id,
                ipaddr=self._host,
                ipport=self._port,
                timeout=self._timeout.total_seconds(),
            )
        else:
            self._logger.info(f"Scanning SunSpec device {self._com_port}#{device_id}")
            # The RTU device registers itself on the shared serial client for this port on
            # construction; its connect() is a no-op
            device = SunSpecModbusClientDeviceRTU(
                slave_id=device_id,
                name=self._com_port,
                baudrate=self._baudrate,
                parity=self._parity,
                timeout=self._timeout.total_seconds(),
            )
        try:
            device.connect()
            # scan() would disconnect afterward if it opened the connection itself
            device.scan(connect=False, full_model_read=True)
        except BaseException:
            # A failed scan never reaches disconnect(), so release the device here. For rtu
            # this also restores the shared client's reference count.
            self.__close_device(device)
            raise

        models = sorted(mid for mid in device.models.keys() if isinstance(mid, int))
        self._logger.info(f"Discovered SunSpec models on device {device_id}: {models}")
        common = device.models.get("common")
        if common:
            info = {key: common[0].points[key].cvalue for key in ("Mn", "Md", "SN", "Vr") if key in common[0].points}
            info = {key: value for key, value in info.items() if value}
            if info:
                self._logger.info(
                    f"SunSpec device {device_id} identity: " + ", ".join(f"{k}={v}" for k, v in info.items())
                )
        return device

    def __bench(self, device_id: int) -> None:
        """Drop a unit's cached scan and hold it out of the bus for the reconnect interval."""
        device = self.__devices.pop(device_id, None)
        if device is not None:
            self.__close_device(device)
        self.__resolved.pop(device_id, None)
        self.__unavailable[device_id] = pd.Timestamp.now(tz.UTC)

    def __resolve(
        self,
        device_id: int,
        device: SunSpecModbusClientDevice,
        resource: Resource,
    ) -> Optional[Tuple[Any, Any]]:
        """Resolve a resource to its (model, point) on an already scanned device, cached."""
        resolved = self.__resolved.setdefault(device_id, {})
        if resource.id in resolved:
            return resolved[resource.id]
        try:
            model_point = self._resolve_point(device, resource)
        except ConfigurationError as e:
            self._logger.warning(f"Invalid SunSpec point configuration for resource '{resource.id}': {e}")
            model_point = None
        resolved[resource.id] = model_point
        return model_point

    # noinspection PyShadowingBuiltins
    def _resolve_point(self, device: SunSpecModbusClientDevice, resource: Resource) -> Tuple[Any, Any]:
        model_key = resource.get("model")
        key = str(model_key).strip()
        if key.isdigit():
            key = int(key)
        models = device.models
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

    # ------------------------------------------------------------------ read / write

    def read(self, resources: Resources) -> pd.DataFrame:
        timestamp = pd.Timestamp.now(tz.UTC).floor(freq="s")
        data = pd.DataFrame(index=[timestamp], columns=resources.ids)
        try:
            for device_id, unit_resources in resources.groupby(self._resource_device):
                if device_id is None:
                    for resource in unit_resources:
                        self._logger.warning(
                            f"Missing or invalid SunSpec channel config key 'device' for resource '{resource.id}'"
                        )
                        data.at[timestamp, resource.id] = ChannelState.ARGUMENT_SYNTAX_ERROR
                    continue

                device = self.__device(device_id)
                if device is None:
                    for resource in unit_resources:
                        data.at[timestamp, resource.id] = ChannelState.NOT_AVAILABLE
                    continue

                self.__read_device(timestamp, data, device_id, device, unit_resources)
            return data

        except ModbusClientTimeout as e:
            # A timeout that escaped the per-unit handling still describes one device, not the
            # transport, so leave the connector connected
            raise ConnectorError(self, e)
        except ModbusClientError as e:
            self.__healthy = False
            raise ConnectionError(self, e)
        except SunSpecModbusClientError as e:
            raise ConnectorError(self, e)
        except IOError as e:
            self.__healthy = False
            raise ConnectionError(self, e)

    def __read_device(
        self,
        timestamp: pd.Timestamp,
        data: pd.DataFrame,
        device_id: int,
        device: SunSpecModbusClientDevice,
        resources: Resources,
    ) -> None:
        blocks: Dict[int, Tuple[Any, List[Resource]]] = {}
        for resource in resources:
            model_point = self.__resolve(device_id, device, resource)
            if model_point is None:
                data.at[timestamp, resource.id] = ChannelState.NOT_AVAILABLE
                continue
            model, _ = model_point
            blocks.setdefault(id(model), (model, []))[1].append(resource)

        pending = list(blocks.values())
        for index, (model, block_resources) in enumerate(pending):
            try:
                # One block read per model keeps values and their scale factors consistent
                model.read()
            except ModbusClientTimeout as e:
                # This unit stopped answering. Bench it and degrade only its own channels;
                # the other units on this transport are untouched.
                self._logger.warning(
                    f"SunSpec device {device_id} timed out reading, benching for {self._interval_reconnect}: {e}"
                )
                self.__bench(device_id)
                for _, unread in pending[index:]:
                    for resource in unread:
                        data.at[timestamp, resource.id] = ChannelState.UNKNOWN_ERROR
                return
            except SunSpecModbusClientError as e:
                self._logger.warning(f"Error reading SunSpec model block of device {device_id}: {e}")
                for resource in block_resources:
                    data.at[timestamp, resource.id] = ChannelState.UNKNOWN_ERROR
                continue

            for resource in block_resources:
                try:
                    value = self.__resolved[device_id][resource.id][1].cvalue
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

    def write(self, data: pd.DataFrame) -> None:
        try:
            for device_id, unit_channels in self.channels.groupby(self._resource_device):
                if device_id is None:
                    for channel in unit_channels:
                        if channel.id in data.columns:
                            self._logger.warning(
                                f"Missing or invalid SunSpec channel config key 'device' for channel '{channel.id}'"
                            )
                    continue

                write_channels = unit_channels.filter(lambda c: c.id in data.columns)
                if len(write_channels) == 0:
                    continue

                device = self.__device(device_id)
                if device is None:
                    self._logger.warning(
                        f"Cannot write to unavailable SunSpec device {device_id}: "
                        f"{', '.join(c.id for c in write_channels)}"
                    )
                    continue

                self.__write_device(data, device_id, device, write_channels)

        except ModbusClientTimeout as e:
            raise ConnectorError(self, e)
        except ModbusClientError as e:
            self.__healthy = False
            raise ConnectionError(self, e)
        except SunSpecModbusClientError as e:
            raise ConnectorError(self, e)
        except IOError as e:
            self.__healthy = False
            raise ConnectionError(self, e)

    def __write_device(
        self,
        data: pd.DataFrame,
        device_id: int,
        device: SunSpecModbusClientDevice,
        channels: Resources,
    ) -> None:
        for channel in channels:
            channel_data = data.loc[:, channel.id].dropna(axis="index", how="all")
            if channel_data.empty:
                continue
            model_point = self.__resolve(device_id, device, channel)
            if model_point is None:
                self._logger.warning(f"Cannot write unresolved SunSpec channel '{channel.id}'")
                continue
            value = channel_data.iloc[-1]
            if hasattr(value, "item"):
                # Unwrap numpy scalars, which pysunspec2's register packing rejects
                value = value.item()
            point = model_point[1]
            try:
                point.cvalue = value
                point.write()

            except (SunSpecModbusValueError, ModelError, TypeError, ValueError) as e:
                self._logger.warning(f"Invalid value writing SunSpec channel '{channel.id}': {e}")
                continue
            except ModbusClientTimeout as e:
                self._logger.warning(
                    f"SunSpec device {device_id} timed out writing, benching for {self._interval_reconnect}: {e}"
                )
                self.__bench(device_id)
                return
