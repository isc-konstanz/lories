# -*- coding: utf-8 -*-
"""
loris.connectors.context
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Callable, Optional, Type, TypeVar, overload

from loris.connectors import Connector
from loris.core import Context, Registrator, RegistratorContext, Registry
from loris.core.configs import ConfigurationException, Configurations, Configurator

C = TypeVar("C", bound=Connector)

registry = Registry[Connector]()


@overload
def register_connector_type(cls: Type[C]) -> Type[C]: ...


@overload
def register_connector_type(
    *alias: Optional[str],
    factory: Callable[..., Type[C]] = None,
    replace: bool = False,
) -> Type[C]: ...


def register_connector_type(
    *args: Optional[Type[C], str],
    **kwargs,
) -> Type[C] | Callable[[Type[C]], Type[C]]:
    args = list(args)
    if len(args) > 0 and isinstance(args[0], type):
        cls = args.pop(0)
        registry.register(cls, *args, **kwargs)
        return cls

    # noinspection PyShadowingNames
    def _register(cls: Type[C]) -> Type[C]:
        registry.register(cls, *args, **kwargs)
        return cls

    return _register


class ConnectorContext(RegistratorContext[Connector], Configurator):
    SECTION: str = "connectors"

    def __init__(self, context: Context, *args, **kwargs) -> None:
        from loris.data.context import DataContext

        if context is None or not isinstance(context, DataContext):
            raise ConfigurationException(f"Invalid data context: {None if context is None else type(context)}")
        super().__init__(context, *args, **kwargs)

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
    ) -> None:
        defaults = {}
        configs = configs.copy()
        if configs.has_section(self.SECTION):
            connectors = configs.get_section(self.SECTION)
            for section in self._get_type().SECTIONS:
                if section in connectors:
                    defaults.update(connectors.pop(section))

            self._load_sections(context, connectors, defaults)
        self._load_from_file(context, configs.dirs, "connectors.conf", defaults)
