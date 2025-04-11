# -*- coding: utf-8 -*-
"""
lori.connectors.context
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Callable, Collection, Optional, Type, TypeVar

from lori.connectors.core import _Connector
from lori.core import Configurations, Context, Registrator, RegistratorContext, Registry

C = TypeVar("C", bound=_Connector)

registry = Registry[_Connector]()


# noinspection PyShadowingBuiltins
def register_connector_type(
    type: str,
    *alias: str,
    factory: Callable[[Context | Registrator, Optional[Configurations]], C] = None,
    replace: bool = False,
) -> Callable[[Type[C]], Type[C]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[C]) -> Type[C]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


class ConnectorContext(RegistratorContext[C]):
    @property
    def _registry(self) -> Registry[C]:
        return registry

    def load(self, configs: Configurations, **kwargs: Any) -> Collection[C]:
        return self._load(self, configs, includes=_Connector.INCLUDES, **kwargs)
