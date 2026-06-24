# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.base
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, List, Optional

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations

# Sentinel - distinguishes "no default provided" from ``default=None``.
# Defined here so every typed subclass and the factory can share the same object.
_UNSET = object()


class _Parameter(ABC):
    """Abstract base for all parameter descriptors.

    Subclasses are collected by ``ConfiguratorMeta`` as class-level attributes
    on ``Configurator`` subclasses and are used to validate / inject defaults
    into a ``Configurations`` object before ``configure()`` is called.
    """

    #: Attribute name on the owning class — injected by ``ConfiguratorMeta``.
    name: str | None
    #: Explicit override for the key in the config file.  Falls back to
    #: ``name`` when *None*.
    key: str | None
    #: Human-readable description (used for documentation and error messages).
    desc: str | None
    #: Whether the parameter (or group) must be present in the configuration.
    required: bool

    def __init__(
        self,
        key: str | None = None,
        desc: str | None = None,
        required: bool = True,
    ) -> None:
        self.name = None  # set later by ConfiguratorMeta
        self.key = key
        self.desc = desc
        self.required = required

    def _resolve_key(self) -> str:
        """Return the effective config-file key for this parameter."""
        return self.key if self.key is not None else self.name

    @abstractmethod
    def resolve(self, configs: "_Configurations") -> Any:
        """Read, coerce and validate this parameter from *configs*.

        Returns the resolved value.  Raises ``ConfigurationError`` on any
        problem (missing required value, wrong type, constraint violation …).
        """
        ...

    def to_schema(self) -> dict:
        """Return a serialisable dict describing this parameter's metadata.

        Intended for UI generation, documentation, and schema export.
        Subclasses extend this dict with type-specific fields.
        """
        return {
            "name": self.name,
            "key": self._resolve_key(),
            "required": self.required,
            "desc": self.desc,
        }

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name!r}, key={self._resolve_key()!r}, required={self.required})"


class _TypedParameter(_Parameter, ABC):
    """Intermediate base for all single-value typed parameter descriptors.

    Carries the shared state (default, choices, validator) and provides a
    concrete :meth:`resolve` implementation that delegates the actual
    value-reading to the abstract :meth:`_get_typed` hook, which every
    typed subclass must implement.
    """

    def __init__(
        self,
        key: str | None = None,
        default: Any = _UNSET,
        required: bool | None = None,
        desc: str | None = None,
        choices: Optional[List[Any]] = None,
        validator: Optional[Callable[[Any], None]] = None,
    ) -> None:
        if required is None:
            required = default is _UNSET
        super().__init__(key=key, desc=desc, required=required)
        self._has_default = default is not _UNSET
        self.default: Any = None if default is _UNSET else default
        self.choices = choices
        self.validator = validator

    def resolve(self, configs: "_Configurations") -> Any:
        """Read, coerce and validate a single key from *configs*."""
        k = self._resolve_key()

        if k not in configs:
            if self.required:
                raise _config_error(
                    f"Missing required configuration key '{k}'" + (f" — {self.desc}" if self.desc else "")
                )
            return self.default if self._has_default else None

        value = self._get_typed(configs, k)
        self._validate(k, value)
        return value

    @abstractmethod
    def _get_typed(self, configs: "_Configurations", key: str) -> Any:
        """Read *key* from *configs* with the appropriate typed accessor."""
        ...

    def _validate(self, key: str, value: Any) -> None:
        """Run choices check and custom validator (if any)."""
        if self.choices is not None and value not in self.choices:
            raise _config_error(f"Invalid value {value!r} for '{key}': must be one of {self.choices}")
        if self.validator is not None:
            try:
                self.validator(value)
            except (ValueError, TypeError) as exc:
                raise _config_error(f"Validation failed for '{key}' (value={value!r}): {exc}") from exc

    def to_schema(self) -> dict:
        schema = super().to_schema()
        schema.update(
            {
                "default": self.default if self._has_default else None,
                "has_default": self._has_default,
                "choices": self.choices,
            }
        )
        return schema

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"name={self.name!r}, key={self._resolve_key()!r}, "
            f"required={self.required}, default={self.default!r})"
        )


def _config_error(msg: str):
    from lories.core.configs.errors import ConfigurationError

    return ConfigurationError(msg)
