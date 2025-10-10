# -*- coding: utf-8 -*-
"""
lori.components.context
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from typing import Callable, Optional, Type

from lori._core._component import Component, _Component, _ComponentContext  # noqa
from lori.core.configs import Configurations
from lori.core.register import RegistratorContext, Registry

registry = Registry[_Component]()


# noinspection PyShadowingBuiltins
def register_component_type(
    type: str,
    *alias: str,
    factory: Callable[[ComponentContext | Component, Optional[Configurations]], Component] = None,
    replace: bool = False,
) -> Callable[[Type[Component]], Type[Component]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[Component]) -> Type[Component]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


class ComponentContext(_ComponentContext, RegistratorContext[Component]):
    @property
    def _registry(self) -> Registry[Component]:
        return registry

    # noinspection PyShadowingBuiltins
    def activate(self, filter: Optional[Callable[[Component], bool]] = None) -> None:
        self._activate(*self.filter(filter))

    def _activate(self, *component: Component) -> None:
        for component in component:
            if not component.is_enabled():
                self._logger.debug(
                    f"Skipping to activate disabled {type(component).__name__} '{component.name}': {component.id}"
                )
                continue
            self.__activate(component)

    def __activate(self, component: Component) -> None:
        self._logger.debug(f"Activating {type(component).__name__} '{component.name}': {component.id}")
        component.activate()

        self._logger.info(f"Activated {type(component).__name__} '{component.name}': {component.id}")

    # noinspection PyShadowingBuiltins
    def deactivate(self, filter: Optional[Callable[[Component], bool]] = None) -> None:
        self._deactivate(*self.filter(filter))

    def _deactivate(self, *components: Component) -> None:
        for component in reversed(list(components)):
            if not component.is_active():
                self._logger.debug(
                    f"Skipping to deactivate already deactivated {type(component).__name__} '{component.name}': "
                    f"{component.id}"
                )
                return
            self.__deactivate(component)

    def __deactivate(self, component: Component) -> None:
        if not component.is_active():
            self._logger.debug(
                f"Skipping to deactivate already deactivated {type(component).__name__} '{component.name}': "
                f"{component.id}"
            )
            return
        try:
            self._logger.debug(f"Deactivating {type(component).__name__} '{component.name}': {component.id}")
            component.deactivate()

            self._logger.info(f"Deactivated {type(component).__name__} '{component.name}': {component.id}")

        except Exception as e:
            self._logger.warning(f"Failed deactivating component '{component.id}': {str(e)}")
            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.exception(e)
