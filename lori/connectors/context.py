# -*- coding: utf-8 -*-
"""
lori.connectors.context
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Callable, Optional, Type, TypeVar

from lori.connectors import Connector
from lori.core import Configurations, Context, Registrator, RegistratorContext, Registry

C = TypeVar("C", bound=Connector)

registry = Registry[Connector]()


# noinspection PyShadowingBuiltins
def register_connector_type(
    type: str,
    *alias: str,
    factory: Callable[[Registrator | Context, Optional[Configurations]], C] = None,
    replace: bool = False,
) -> Callable[[Type[C]], Type[C]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[C]) -> Type[C]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


class ConnectorContext(RegistratorContext[Connector]):
    SECTION: str = "connectors"

    @property
    def _registry(self) -> Registry[Connector]:
        return registry

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._load(self, configs)

    def _load(
        self,
        context: Registrator | RegistratorContext,
        configs: Configurations,
        configs_file: str = "connectors.conf",
    ) -> None:
        defaults = {}
        configs = configs.copy()
        if configs.has_section(self.SECTION):
            connectors = configs.get_section(self.SECTION)
            for section in Connector.SECTIONS:
                if section in connectors:
                    defaults.update(connectors.pop(section))

            self._load_sections(context, connectors, defaults, Connector.SECTIONS)
        self._load_from_file(context, configs.dirs, configs_file, defaults)
