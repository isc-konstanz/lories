# -*- coding: utf-8 -*-
"""
lories.connectors.connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import inspect
import logging
from collections import OrderedDict
from collections.abc import Callable
from functools import wraps
from threading import Lock
from typing import Any, Dict, Optional

import pandas as pd
import pytz as tz
from lories._core._configurations import Configurations  # noqa
from lories._core._connector import ConnectType, _Connector  # noqa
from lories._core._context import _Context  # noqa
from lories._core._registrator import RegistratorContext  # noqa
from lories.connectors.errors import ConnectionError, ConnectorError
from lories.core import Resource, ResourceError, Resources
from lories.core.configs.configurator import _WH, _WK, _WR, Configurator, ConfiguratorMeta
from lories.core.configs.errors import ConfigurationError
from lories.core.configs.parameters import Parameter
from lories.core.register.registrator import Registrator
from lories.data.channels import Channel, Channels, ChannelState
from lories.data.validation import validate_index


class ConnectorMeta(ConfiguratorMeta):
    __channel_parameters__: Dict[str, Any]

    def __new__(mcls, name, bases, namespace):
        from lories.core.configs.parameters.resource_parameter import ResourceParameter

        # Inherit channel parameters from all parent classes (left-to-right MRO).
        channel_params: Dict[str, Any] = {}
        for base in bases:
            channel_params.update(getattr(base, "__channel_parameters__", {}))

        # Collect ResourceParameter descriptors declared in this class body.
        for attr, value in namespace.items():
            if isinstance(value, ResourceParameter):
                if value.name is None:
                    value.name = attr
                channel_params[attr] = value

        namespace["__channel_parameters__"] = channel_params
        return super().__new__(mcls, name, bases, namespace)

    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        connector = super().__call__(*args, **kwargs)
        cls._wrap_method(connector, "connect")
        cls._wrap_method(connector, "disconnect")
        cls._wrap_method(connector, "read")
        cls._wrap_method(connector, "write")

        return connector


# noinspection PyAbstractClass
class Connector(_Connector, Registrator, metaclass=ConnectorMeta):
    _connect = Parameter(key="connect", type=bool, default=True, desc="Connect on activation")

    _connected: bool = False
    _connect_type: ConnectType = ConnectType.AUTO

    _timestamp_connect: pd.Timestamp = pd.NaT
    _timestamp_disconnect: pd.Timestamp = pd.NaT
    _interval_reconnect: pd.Timedelta = pd.Timedelta(minutes=1)

    __resources: Resources

    _lock_timeout: int = 60
    _lock: Lock

    # Set to False on a subclass to suppress channel-config tracing even at DEBUG level.
    _WARN_UNDECLARED_CHANNEL_CONFIGS: bool = True
    # Keys always present in a channel's connector config that are never user-declared.
    _CHANNEL_CONFIGS_RESERVED_KEYS: frozenset = frozenset({"enabled"})

    def __init__(
        self,
        context: RegistratorContext,
        configs: Optional[Configurations] = None,
        **kwargs,
    ) -> None:
        super().__init__(context=context, configs=configs, **kwargs)
        self.__resources = Resources()
        self._lock = Lock()

    def __getstate__(self) -> Dict[str, Any]:
        state = super().__getstate__()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        state["_lock"] = Lock()
        super().__setstate__(state)

    def __enter__(self) -> Connector:
        self.connect(self.__resources)
        return self

    # noinspection PyShadowingBuiltins
    def __exit__(self, type, value, traceback):
        self.disconnect()

    @classmethod
    def _assert_context(cls, context: RegistratorContext) -> RegistratorContext:
        if context is None:
            raise TypeError(f"Invalid '{cls.__name__}' context: {type(context)}")
        return super()._assert_context(context)

    # noinspection PyShadowingBuiltins, PyProtectedMember
    def _get_vars(self) -> Dict[str, Any]:
        vars = super()._get_vars()

        # Channels are a subset of resources, hence omit them from printing
        vars.pop("channels", None)
        return vars

    # noinspection PyShadowingBuiltins
    def _convert_vars(self, convert: Callable = str) -> Dict[str, str]:
        vars = self._get_vars()
        values = OrderedDict()
        try:
            id = vars.pop("id", self.id)
            key = vars.pop("key", self.key)
            if id != key:
                values["id"] = id
            values["key"] = key
        except (ResourceError, AttributeError):
            # Abstract properties are not yet instanced
            pass

        if "name" in vars:
            values["name"] = vars.pop("name")

        values.update(
            {
                k: str(v) if not isinstance(v, (_Context, Configurator, Resource, Resources)) else convert(v)
                for k, v in vars.items()
            }
        )
        values["context"] = convert(self.context)
        values["configurations"] = convert(self.configs)
        values["configured"] = str(self.is_configured())
        values["connected"] = str(self._is_connected())
        values["enabled"] = str(self.is_enabled())
        return values

    @property
    def resources(self) -> Resources:
        return self.__resources

    @property
    def channels(self) -> Channels:
        return Channels([resource for resource in self.__resources if isinstance(resource, Channel)])

    def set_channels(self, state: ChannelState) -> None:
        # Set only channel states for channels, that actively are getting read or written by this connector.
        # Local channels may be logging channels as well, which need to be skipped.
        for channel in self.channels.filter(lambda c: c.has_connector(self.id)):
            channel.state = state

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._connect_type = ConnectType.get(self._connect)

    def _is_disconnected(self) -> bool:
        return not self._is_connected()

    # noinspection SpellCheckingInspection
    def _is_reconnectable(self) -> bool:
        if not self.is_enabled() or not self.is_configured() or not self._is_connectable():
            return False
        if not pd.isna(self._timestamp_connect):
            return self._timestamp_connect + self._interval_reconnect <= pd.Timestamp.now(tz.UTC)
        if not pd.isna(self._timestamp_disconnect):
            return self._timestamp_disconnect + self._interval_reconnect <= pd.Timestamp.now(tz.UTC)
        return False

    def _is_connectable(self) -> bool:
        return self._is_disconnected() and self._connect_type == ConnectType.AUTO

    def _is_connected(self) -> bool:
        return self.is_connected() and self._connected

    def is_connected(self) -> bool:
        return True

    # noinspection PyUnresolvedReferences, PyTypeChecker
    @wraps(_Connector.connect, updated=())
    def _do_connect(self, resources: Resources, locking: bool = True, *args, **kwargs) -> None:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(self, f"Timeout acquiring lock for connecting {type(self).__name__}: {self.id}")
            if not self.is_enabled():
                raise ConfigurationError(f"Trying to connect disabled {type(self).__name__}: {self.id}")
            if not self.is_configured():
                raise ConfigurationError(f"Trying to connect unconfigured {type(self).__name__}: {self.id}")

            self._timestamp_connect = pd.Timestamp.now(tz.UTC)
            self._timestamp_disconnect = pd.NaT

            if not self._is_connected():
                self._at_connect(resources)
                self._run_connect(resources, *args, **kwargs)
                self._on_connect(resources)
                self.__resources = resources
            else:
                self._logger.warning(f"{type(self).__name__} '{self.id}' already connected")

            self._connected = True
        finally:
            self._lock.release()

    @classmethod
    def _warn_undeclared_channel_configs(cls, resources: Resources) -> None:
        """Log a DEBUG message for every per-channel connector config key that
        has no matching :class:`~lories.core.configs.parameters.ChannelParameter`
        declaration on this connector class.

        Called automatically from :meth:`_at_connect` — silent above DEBUG level
        and disabled per-class via ``_WARN_UNDECLARED_CHANNEL_CONFIGS = False``.
        """
        if not cls._WARN_UNDECLARED_CHANNEL_CONFIGS:
            return
        logger = logging.getLogger(cls.__module__)
        if not logger.isEnabledFor(logging.DEBUG):
            return

        declared: frozenset = frozenset(p._resolve_key() for p in getattr(cls, "__channel_parameters__", {}).values())

        try:
            cls_file = inspect.getfile(cls)
        except (TypeError, OSError):
            cls_file = "<unknown file>"

        for resource in resources:
            channel_connector = getattr(resource, "connector", None)
            if channel_connector is None or not getattr(channel_connector, "enabled", False):
                continue
            # Only inspect channels that are wired to this connector class.
            registrator = channel_connector._get_registrator()
            if registrator is not None and not isinstance(registrator, cls):
                continue
            channel_configs = channel_connector._copy_configs()
            for key in channel_configs:
                if key in cls._CHANNEL_CONFIGS_RESERVED_KEYS:
                    continue
                if key not in declared:
                    logger.debug(
                        f"{_WH}%s (module: %s, file: %s): "
                        f"channel '%s' uses config key '{_WK}%s{_WH}' "
                        f"with no ChannelParameter declaration{_WR}",
                        cls.__name__,
                        cls.__module__,
                        cls_file,
                        resource.id,
                        key,
                    )

    def _resolve_channel_params(self, resources: Resources) -> None:
        """Resolve per-channel parameters for *resources* and write them back.

        Iterates all ``ResourceParameter`` declarations in
        ``__channel_parameters__``, calls ``param.resolve(channel_configs)`` for
        each channel, and stores the typed results into the live channel-connector
        config dict via :meth:`~lories.data.channels._core._ChannelWrapper._set_resolved`.

        The merged config dict (resource-level + connector-level) is used so that
        the flat INI/TOML format (keys at channel level) and the nested format
        (keys inside a ``connector:`` sub-dict) are both supported.

        Raises ``ConfigurationError`` for any missing required key or constraint
        violation (delegated to ``ResourceParameter.resolve()``).
        """
        from lories.core.configs.parameters.resource_parameter import ResourceParameter

        channel_params = type(self).__channel_parameters__
        if not channel_params:
            return

        for resource in resources:
            channel_connector = getattr(resource, "connector", None)
            if channel_connector is None or not channel_connector.enabled:
                continue
            registrator = channel_connector._get_registrator()
            if registrator is not None and not isinstance(registrator, type(self)):
                continue

            # Merge connector-level configs (nested format) with resource-level
            # configs (flat format) so that both layouts are supported.
            channel_configs = {**channel_connector._copy_configs(), **resource._copy_configs()}
            resolved = {
                param._resolve_key(): param.resolve(channel_configs)
                for attr, param in channel_params.items()
                if isinstance(param, ResourceParameter)
            }
            channel_connector._set_resolved(resolved)

    def _at_connect(self, resources: Resources) -> None:
        type(self)._warn_undeclared_channel_configs(resources)
        self._resolve_channel_params(resources)

    def _on_connect(self, resources: Resources) -> None:
        pass

    # noinspection PyUnresolvedReferences, PyTypeChecker
    @wraps(_Connector.disconnect, updated=())
    def _do_disconnect(self, locking: bool = True) -> None:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(self, f"Timeout acquiring lock for disconnecting {type(self).__name__}: {self.id}")

            self._timestamp_connect = pd.NaT
            self._timestamp_disconnect = pd.Timestamp.now(tz.UTC)

            if not self._is_disconnected():
                self._at_disconnect()
                self._run_disconnect()
                self._on_disconnect()

            self._connected = False
        finally:
            self._lock.release()

    def _at_disconnect(self) -> None:
        pass

    def _on_disconnect(self) -> None:
        pass

    # noinspection PyUnresolvedReferences, PyTypeChecker
    @wraps(_Connector.read, updated=())
    def _do_read(self, resources: Resources, locking: bool = True, *args, **kwargs) -> pd.DataFrame:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(self, f"Timeout acquiring lock for reading {type(self).__name__}: {self.id}")
            if not self._is_connected():
                raise ConnectionError(self, f"Trying to read from unconnected {type(self).__name__}: {self.id}")

            data = self._run_read(resources, *args, **kwargs)
            data = self._validate(resources, data)
            return data

        finally:
            self._lock.release()

    # noinspection PyMethodMayBeStatic
    def _validate(self, resources: Resources, data: pd.DataFrame) -> pd.DataFrame:
        if not data.empty:
            data = validate_index(data)
            for resource in resources:
                if resource.id not in data:
                    continue
                if resource.type in [pd.Timestamp, dt.datetime]:
                    resource_data = data[resource.id]
                    if pd.api.types.is_string_dtype(resource_data.values):
                        data[resource.id] = pd.to_datetime(resource_data)
        return data

    # noinspection PyUnresolvedReferences, PyTypeChecker
    @wraps(_Connector.write, updated=())
    def _do_write(self, data: pd.DataFrame, locking: bool = True, *args, **kwargs) -> None:
        try:
            if not self._lock.acquire(blocking=locking, timeout=self._lock_timeout):
                raise ConnectorError(self, f"Timeout acquiring lock for writing {type(self).__name__}: {self.id}")
            if not self._is_connected():
                raise ConnectionError(self, f"Trying to write to unconnected {type(self).__name__}: {self.id}")
            unknown = [c for c in data.columns if c not in self.resources]
            if len(unknown) > 0:
                raise ConnectorError(
                    self,
                    f"Trying to write unknown resource{'s' if len(unknown) > 0 else ''} '{', '.join(unknown)}' for "
                    f"{type(self).__name__}: {self.id}",
                )

            self._run_write(data, *args, **kwargs)
        finally:
            self._lock.release()
