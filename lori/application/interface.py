# -*- coding: utf-8 -*-
"""
lori.application.interface
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import Callable
from typing import Optional, Type, TypeVar

from lori._core._configurations import Configurations  # noqa
from lori._core._data import DataContext, _DataContext, _DataManager  # noqa
from lori.core import ConfigurationError, Registry, ResourceError
from lori.core.configs.configurator import Configurator, ConfiguratorMeta


# noinspection PyShadowingBuiltins
def register_interface_type(
    type: str,
    *alias: str,
    factory: Callable[[DataContext, Configurations], InterfaceType] = None,
    replace: bool = False,
) -> Callable[[Type[InterfaceType]], Type[InterfaceType]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[InterfaceType]) -> Type[InterfaceType]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


class InterfaceMeta(ConfiguratorMeta):
    def __call__(cls, context: DataContext, configs: Configurations, **kwargs) -> InterfaceType:
        global _instance

        _type = configs.get("type", default="default").lower()
        _cls = cls._get_class(_type)
        if cls != _cls:
            return _cls(context, configs, **kwargs)
        if _instance is None:
            _instance = super().__call__(context, configs, **kwargs)
        return _instance

    # noinspection PyMethodMayBeStatic, PyShadowingBuiltins
    def _get_class(cls: Type[InterfaceType], type: str) -> Type[InterfaceType]:
        if type == "default":
            return cls
        elif registry.has_type(type):
            registration = registry.from_type(type)
            return registration.type

        raise InterfaceException(f"Unknown interface type '{type}'")


class Interface(Configurator, metaclass=InterfaceMeta):
    TYPE: str = "interface"

    __context: _DataContext

    # noinspection PyUnresolvedReferences
    def __init__(self, context: DataContext, configs: Configurations, **kwargs) -> None:
        super().__init__(configs, **kwargs)
        self.__context = self._assert_context(context)

    @classmethod
    def _assert_context(cls, context: DataContext) -> DataContext:
        if context is None or not isinstance(context, _DataManager):
            raise ResourceError(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    @classmethod
    def _assert_configs(cls, configs: Optional[Configurations]) -> Optional[Configurations]:
        if configs is None:
            raise ConfigurationError(f"Invalid '{cls.__name__}' configurations: {type(configs)}")
        return super()._assert_configs(configs)

    @property
    def context(self) -> DataContext:
        return self.__context

    def start(self, *args, **kwargs) -> None:
        pass


class InterfaceException(ResourceError):
    """
    Raise if an error occurred accessing the interface.

    """


InterfaceType = TypeVar("InterfaceType", bound=Interface)

# FIXME: Add global variable typing again once Python >= 3.9 is a requirement
# _instance: Optional[InterfaceType] = None
_instance = None
registry = Registry[Interface]()
