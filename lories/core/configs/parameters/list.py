# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.list
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ListParameter`` — reads a configuration key as a typed list.

Handles two source formats transparently:

* **TOML array** — the TOML loader already returns a Python ``list``;
  each element is coerced to *item_type*.
* **Comma-separated string** — ``"80, 443, 8080"`` is split on commas,
  each token stripped and coerced to *item_type*.

An optional per-element *item_validator* is called on each resolved
element before the whole-list *validator* runs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, List, Optional, Type

from lories.core.configs.parameters.base import _UNSET, _config_error, _TypedParameter

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations


class ListParameter(_TypedParameter):
    """Parameter for homogeneous list configuration values.

    Usage::

        class MyConfigurator(Configurator):
            ports   = ListParameter(item_type=int, default=[1883], desc="Broker ports")
            tags    = ListParameter(item_type=str, desc="Resource tags")
            factors = ListParameter(item_type=float, default=[0.5, 1.0], desc="Scaling factors")

    Or via the ``Parameter`` factory with a ``List[T]`` generic alias::

        from typing import List
        ports = Parameter(type=List[int], default=[1883], desc="Broker ports")
        tags  = Parameter(type=List[str], desc="Resource tags")

    Parameters
    ----------
    item_type:
        The type each element will be coerced to.  Defaults to ``str``.
    item_validator:
        Optional callable called on each individual element after
        type-coercion.  Raises ``ValueError`` / ``TypeError`` on failure.
    """

    def __init__(
        self,
        item_type: Type = str,
        key: str | None = None,
        default: Any = _UNSET,
        required: bool | None = None,
        desc: str | None = None,
        choices: Optional[List[Any]] = None,
        validator: Optional[Callable[[Any], None]] = None,
        item_validator: Optional[Callable[[Any], None]] = None,
    ) -> None:
        super().__init__(
            key=key,
            default=default,
            required=required,
            desc=desc,
            choices=choices,
            validator=validator,
        )
        self.item_type = item_type
        self.item_validator = item_validator

    def _get_typed(self, configs: "_Configurations", key: str) -> list:
        raw = configs.get(key)
        return self._coerce(key, raw)

    def _coerce(self, key: str, raw: Any) -> list:
        """Convert *raw* to ``List[item_type]``.

        Accepts:
        * An already-parsed Python ``list`` (e.g. from a TOML array).
        * A comma-separated ``str`` (e.g. ``"80, 443"``).
        * A single scalar that should become a one-element list.
        """
        if isinstance(raw, list):
            items = raw
        elif isinstance(raw, str):
            items = [tok.strip() for tok in raw.split(",") if tok.strip()]
        else:
            items = [raw]

        result: list = []
        for i, item in enumerate(items):
            try:
                coerced = item if isinstance(item, self.item_type) else self.item_type(item)
            except (TypeError, ValueError) as exc:
                raise _config_error(
                    f"Cannot convert element [{i}] of '{key}' " f"({item!r}) to {self.item_type.__name__}: {exc}"
                ) from exc

            if self.item_validator is not None:
                try:
                    self.item_validator(coerced)
                except (ValueError, TypeError) as exc:
                    raise _config_error(
                        f"Element [{i}] of '{key}' (value={coerced!r}) " f"failed item validation: {exc}"
                    ) from exc

            result.append(coerced)

        return result

    def to_schema(self) -> dict:
        return {
            **super().to_schema(),
            "type": "list",
            "item_type": self.item_type.__name__,
        }

    def __repr__(self) -> str:
        return (
            f"ListParameter("
            f"name={self.name!r}, key={self._resolve_key()!r}, "
            f"item_type={self.item_type.__name__}, "
            f"required={self.required}, default={self.default!r})"
        )
