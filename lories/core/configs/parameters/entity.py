# -*- coding: utf-8 -*-
"""
lories.core.configs.parameters.entity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Declarative descriptors for nested *components* and *connectors*.

These mirror :class:`ParameterGroup` but reference a ``Configurator`` subclass
(``Component`` or ``Connector``) rather than a fixed list of children.  They
are *declarative-only*: actual loading still goes through ``ComponentAccess``
/ ``ConnectorAccess``.  Their job is to feed ``to_schema()`` so that docs
pages and the configs editor can render entity slots recursively, and to
expose the list of *fitting* registered types for "add child" UI affordances.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Optional

from lories.core.configs.parameters.base import _Parameter

if TYPE_CHECKING:
    from lories._core._configurations import _Configurations


class _EntityParameter(_Parameter):
    """Base for ``ComponentParameter`` / ``ConnectorParameter``.

    Stores a reference to the expected entity class (or its dotted path, for
    lazy import) and a ``multiple`` flag indicating whether this slot accepts
    a list of children rather than a single one.

    ``resolve()`` is intentionally a no-op: the actual loading of children
    happens dynamically in ``ComponentAccess`` / ``ConnectorAccess``.  The
    descriptor exists only to make the relationship visible to ``to_schema()``
    and, by extension, the GUI.
    """

    #: Subclass key — overridden by concrete entity parameter types.
    _schema_type: str = "entity"
    #: Module path of the registry to consult for fitting types.
    _registry_module: Optional[str] = None

    def __init__(
        self,
        cls: type | str | None = None,
        key: str | None = None,
        desc: str | None = None,
        required: bool = False,
        multiple: bool = False,
    ) -> None:
        super().__init__(key=key, desc=desc, required=required)
        self._cls_ref = cls
        self.multiple = multiple

    @property
    def cls(self) -> Optional[type]:
        """Resolve and return the referenced class (or ``None``)."""
        ref = self._cls_ref
        if ref is None:
            return None
        if isinstance(ref, str):
            module_path, _, name = ref.rpartition(".")
            if not module_path:
                return None
            try:
                resolved = getattr(import_module(module_path), name)
            except (ImportError, AttributeError):
                return None
            self._cls_ref = resolved
            return resolved
        return ref

    def resolve(self, configs: "_Configurations") -> Any:
        """Declarative-only — actual loading is handled elsewhere."""
        return None

    def _get_registry(self):
        if self._registry_module is None:
            return None
        try:
            return import_module(self._registry_module).registry
        except (ImportError, AttributeError):
            return None

    def _fitting_types(self) -> list[dict]:
        """Return registered entries whose ``type`` is a subclass of :attr:`cls`."""
        cls = self.cls
        registry = self._get_registry()
        if cls is None or registry is None:
            return []
        out: list[dict] = []
        for key in sorted(registry.get_types()):
            registration = registry.from_type(key)
            try:
                if not issubclass(registration.type, cls):
                    continue
            except TypeError:
                continue
            out.append(
                {
                    "key": registration.key,
                    "alias": list(getattr(registration, "alias", ()) or ()),
                    "cls": registration.type.__name__,
                    "module": registration.type.__module__,
                    "available": getattr(registration, "available", True),
                }
            )
        return out

    def to_schema(self) -> dict:
        schema = super().to_schema()
        schema["type"] = self._schema_type
        schema["multiple"] = self.multiple
        cls = self.cls
        if cls is not None:
            schema["cls"] = cls.__name__
            schema["module"] = cls.__module__
            children = getattr(cls, "__config_parameters__", {})
            schema["children"] = {k: c.to_schema() for k, c in children.items()}
        else:
            schema["cls"] = None
            schema["module"] = None
            schema["children"] = {}
        schema["fitting_types"] = self._fitting_types()
        return schema

    def __repr__(self) -> str:
        cls = self.cls
        cls_name = cls.__name__ if cls is not None else None
        return (
            f"{type(self).__name__}("
            f"name={self.name!r}, key={self._resolve_key()!r}, "
            f"cls={cls_name!r}, multiple={self.multiple}, required={self.required})"
        )


class ComponentParameter(_EntityParameter):
    """Declares a nested component slot on a ``Configurator`` subclass.

    Usage::

        class System(Component):
            inverter = ComponentParameter(cls=Inverter, desc="Main inverter")
            sensors = ComponentParameter(cls=Sensor, multiple=True)

    The descriptor does not load the child component — that still happens
    through :class:`ComponentAccess` from glob-loaded ``.conf`` files and TOML
    sub-tables.  It exists so that ``to_schema()`` reports the slot, its
    expected type, and every registered component type compatible with it.
    """

    _schema_type = "component"
    _registry_module = "lories.components.context"


class ConnectorParameter(_EntityParameter):
    """Declares a nested connector slot on a ``Configurator`` subclass.

    Usage::

        class System(Component):
            db = ConnectorParameter(cls=Database, desc="Time-series store")
            inputs = ConnectorParameter(cls=Connector, multiple=True)

    See :class:`ComponentParameter` for semantics — this is the connector
    counterpart, consulting the connector registry for fitting types.
    """

    _schema_type = "connector"
    _registry_module = "lories.connectors.context"
