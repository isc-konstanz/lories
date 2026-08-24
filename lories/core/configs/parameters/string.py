# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``StringParameter`` — reads a configuration key as a plain string via
``configs.get()``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, List, Optional

from lories.core.configs.parameters.base import _UNSET, _TypedParameter

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations


class StringParameter(_TypedParameter):
    """Parameter for ``str`` configuration values.

    Reads the raw value from the config file without any type conversion —
    ``configs.get()`` already returns strings for scalar keys.

    Set ``secret=True`` to mark a credential-bearing field (password, token,
    api-key, …).  The flag is exposed via :meth:`to_schema` so UI consumers
    (e.g. the Dash parameter view) can refuse to render the live value, and
    the schema's ``default`` is replaced with ``"********"`` so a hardcoded
    default credential does not leak through the schema either.

    Usage::

        class MyConfigurator(Configurator):
            host     = StringParameter(default="localhost", desc="Broker hostname")
            topic    = StringParameter(desc="MQTT topic to subscribe")
            password = StringParameter(desc="Broker password", secret=True)
    """

    def __init__(
        self,
        key: str | None = None,
        default: Any = _UNSET,
        required: bool | None = None,
        desc: str | None = None,
        choices: Optional[List[Any]] = None,
        validator: Optional[Callable[[Any], None]] = None,
        secret: bool = False,
    ) -> None:
        super().__init__(
            key=key,
            default=default,
            required=required,
            desc=desc,
            choices=choices,
            validator=validator,
        )
        self.secret = secret

    def _get_typed(self, configs: "_Configurations", key: str) -> str:
        return configs.get(key)

    def to_schema(self) -> dict:
        schema = {**super().to_schema(), "type": "str", "secret": self.secret}
        if self.secret and self._has_default:
            schema["default"] = "********"
        return schema

    def __repr__(self) -> str:
        secret_repr = ", secret=True" if self.secret else ""
        return (
            f"{type(self).__name__}("
            f"name={self.name!r}, key={self._resolve_key()!r}, "
            f"required={self.required}, default={self.default!r}{secret_repr})"
        )
