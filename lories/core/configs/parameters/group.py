# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ParameterGroup`` — declares a nested configuration section on a
``Configurator`` subclass and recursively resolves its children.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence

from lories.core.configs.parameters.base import _config_error, _Parameter

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations


class ParameterGroup(_Parameter):
    """Declares a nested configuration section on a ``Configurator`` subclass.

    A ``ParameterGroup`` maps to a TOML sub-table (i.e. a call to
    ``configs.get_member(key)``).  Its *children* are other ``Parameter`` or
    ``ParameterGroup`` instances that are resolved against that sub-section.

    Usage::

        class MyConfigurator(Configurator):
            auth = ParameterGroup(
                required=False,
                desc="Optional authentication credentials",
                children=[
                    Parameter(key="username", type=str, desc="Username"),
                    Parameter(key="password", type=str, desc="Password"),
                ],
            )

    Notes
    -----
    *required* defaults to ``False`` for groups because an entire section
    being absent is a common pattern (optional feature blocks).

    Children whose ``name`` is not yet set (i.e. declared inline with only
    ``key``) have their ``name`` initialised from ``key`` automatically.
    """

    def __init__(
        self,
        key: str | None = None,
        desc: str | None = None,
        required: bool = False,
        children: Optional[Sequence[_Parameter]] = None,
    ) -> None:
        super().__init__(key=key, desc=desc, required=required)

        self.children: Dict[str, _Parameter] = {}
        if children:
            for child in children:
                if child.name is None:
                    child.name = child.key
                self.children[child._resolve_key()] = child

    def add_child(self, name: str, param: _Parameter) -> "ParameterGroup":
        """Register *param* as a child of this group under *name*."""
        if param.name is None:
            param.name = name
        self.children[param._resolve_key()] = param
        return self

    def resolve(self, configs: "_Configurations") -> Optional[Dict[str, Any]]:
        """Validate the sub-section and recursively resolve all children.

        Returns a ``{key: value}`` dict of resolved children, or ``None``
        when the group is optional and absent.
        """
        k = self._resolve_key()

        if not configs.has_member(k):
            if self.required:
                raise _config_error(
                    f"Missing required configuration section '[{k}]'" + (f" — {self.desc}" if self.desc else "")
                )
            return None

        member = configs.get_member(k)
        return {key: child.resolve(member) for key, child in self.children.items()}

    def to_schema(self) -> dict:
        schema = super().to_schema()
        schema["type"] = "group"
        schema["children"] = {k: child.to_schema() for k, child in self.children.items()}
        return schema

    def __repr__(self) -> str:
        return (
            f"ParameterGroup("
            f"name={self.name!r}, key={self._resolve_key()!r}, "
            f"required={self.required}, "
            f"children={list(self.children.keys())})"
        )
