# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.custom
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``CustomParameter`` — fallback for arbitrary types not covered by the
built-in typed subclasses.  Uses ``lories.util.parse_type`` to accept both
Python type objects and dotted-string type names (e.g. ``"mymodule.MyClass"``),
then calls the resolved constructor on the raw config value.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Type

from lories.core.configs.parameters.base import _UNSET, _config_error, _TypedParameter
from lories.util import parse_type

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations


class CustomParameter(_TypedParameter):
    """Parameter for arbitrary / custom types.

    Reads the raw string value from the config file and passes it through the
    constructor of the resolved *type*.  ``lories.util.parse_type`` is used to
    resolve string type names (e.g. ``"mypackage.MyEnum"``) in addition to
    plain type objects.

    Usage::

        class MyConfigurator(Configurator):
            mode  = CustomParameter(type=MyEnum, desc="Operating mode")
            codec = CustomParameter(type="mypackage.Codec", desc="Codec class")
    """

    def __init__(
        self,
        type: Type | str,  # noqa: A002
        key: str | None = None,
        default: Any = _UNSET,
        required: bool | None = None,
        desc: str | None = None,
        choices=None,
        validator=None,
    ) -> None:
        super().__init__(
            key=key,
            default=default,
            required=required,
            desc=desc,
            choices=choices,
            validator=validator,
        )
        self.type: Type = parse_type(type)

    def _get_typed(self, configs: "_Configurations", key: str) -> Any:
        raw = configs.get(key)
        try:
            return self.type(raw)
        except (TypeError, ValueError) as exc:
            raise _config_error(f"Cannot convert '{key}' value {raw!r} to {self.type.__name__}") from exc

    def to_schema(self) -> dict:
        return {**super().to_schema(), "type": self.type.__name__}

    def __repr__(self) -> str:
        return (
            f"CustomParameter("
            f"name={self.name!r}, key={self._resolve_key()!r}, "
            f"type={self.type}, required={self.required}, "
            f"default={self.default!r})"
        )
