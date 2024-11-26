# -*- coding: utf-8 -*-
"""
lori.components.context
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Callable, Optional, Type, TypeVar

from lori.components import Component
from lori.core import Configurations, Context, Registrator, RegistratorContext, Registry

C = TypeVar("C", bound=Component)

registry = Registry[Component]()


# noinspection PyShadowingBuiltins
def register_component_type(
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


class ComponentContext(RegistratorContext[Component]):
    SECTION: str = "components"

    @property
    def _registry(self) -> Registry[Component]:
        return registry

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._load(self, configs)

    def _load(
        self,
        context: Registrator | RegistratorContext,
        configs: Configurations,
        configs_file: str = "components.conf",
    ) -> None:
        defaults = {}
        configs = configs.copy()
        if configs.has_section(self.SECTION):
            components = configs.get_section(self.SECTION)
            for section in Component.SECTIONS:
                if section in components:
                    defaults.update(components.pop(section))

            self._load_sections(context, components, defaults, Component.SECTIONS)

        context_dirs = [
            str(c.configs.dirs.conf)
            for c in self.context.components.values()
            if c != self and isinstance(c, RegistratorContext)
        ]
        if str(configs.dirs.conf) not in context_dirs:
            self._load_from_file(context, configs.dirs, configs_file, defaults)
            self._load_from_dir(context, str(configs.dirs.conf), defaults)
