# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.channel_parameter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ChannelParameter`` — descriptor for declaring per-channel config keys
on a ``Connector`` subclass.
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional, Type

from lories.core.configs.errors import ConfigurationError
from lories.util import to_bool

_UNSET = object()


class ChannelParameter:
    """Declares a per-channel config key expected by a ``Connector`` subclass.

    These are the extra keys that live in a channel's connector-side config
    dict (e.g. ``address``, ``function``, ``device`` for Modbus; ``topic`` for
    MQTT).  ``ConnectorMeta`` collects them into ``__channel_parameters__`` and
    ``Connector._warn_undeclared_channel_configs()`` uses them at connect-time
    to warn about channel config keys that have no declaration.

    Intentionally **not** a subclass of ``_Parameter`` so that
    ``ConfiguratorMeta`` does not accidentally add it to
    ``__config_parameters__``.

    Usage::

        class ModbusClient(Connector):
            address  = ChannelParameter(type=int, desc="Register start address")
            function = ChannelParameter(
                type=str, required=False, default="holding_register",
                desc="Modbus function: holding_register | input_register | coil",
            )
            device   = ChannelParameter(type=int, required=False, desc="Slave device ID")

        class MqttConnector(Connector):
            topic = ChannelParameter(type=str, desc="MQTT topic for this channel")
    """

    #: Attribute name on the owning class — injected by ``ConnectorMeta``.
    name: str | None
    #: Explicit override for the key in the channel config dict.
    key: str | None
    #: Human-readable description.
    desc: str | None
    #: Whether the channel config key is required.
    required: bool

    def __init__(
        self,
        key: str | None = None,
        type: Type | None = None,  # noqa: A002
        default: Any = _UNSET,
        required: bool | None = None,
        desc: str | None = None,
        choices: Optional[List[Any]] = None,
        validator: Optional[Callable[[Any], None]] = None,
    ) -> None:
        if required is None:
            required = default is _UNSET
        self.name = None  # set later by ConnectorMeta
        self.key = key
        self.desc = desc
        self.required = required
        self.type = type
        self._has_default = default is not _UNSET
        self.default: Any = None if default is _UNSET else default
        self.choices = choices
        self.validator = validator

    def _resolve_key(self) -> str:
        """Return the effective channel config key for this parameter."""
        return self.key if self.key is not None else self.name

    def resolve(self, channel_configs: dict) -> Any:
        """Resolve this parameter from *channel_configs*.

        Returns the typed value, or the default / ``None`` when the key is
        absent and the parameter is optional.  Raises :exc:`ConfigurationError`
        for missing required keys, invalid choices, and validator failures.
        """
        key = self._resolve_key()
        if key not in channel_configs:
            if self.required:
                raise ConfigurationError(
                    f"Missing required channel config key '{key}'" + (f" — {self.desc}" if self.desc else "")
                )
            return self.default if self._has_default else None

        raw = channel_configs[key]
        value = self._cast(raw)
        self._validate_value(key, value)
        return value

    def _cast(self, raw: Any) -> Any:
        if self.type is None or isinstance(raw, self.type):
            return raw
        try:
            if self.type is bool:
                return to_bool(raw, any_str=True)
            return self.type(raw)
        except (ValueError, TypeError) as exc:
            raise ConfigurationError(
                f"Cannot cast channel config value {raw!r} to {self.type.__name__}: {exc}"
            ) from exc

    def _validate_value(self, key: str, value: Any) -> None:
        if self.choices is not None and value not in self.choices:
            raise ConfigurationError(f"Invalid value {value!r} for '{key}': must be one of {self.choices}")
        if self.validator is not None:
            try:
                self.validator(value)
            except (ValueError, TypeError) as exc:
                raise ConfigurationError(f"Validation failed for '{key}' (value={value!r}): {exc}") from exc

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(" f"name={self.name!r}, key={self._resolve_key()!r}, " f"required={self.required})"
        )
