# -*- coding: utf-8 -*-
"""
lori.application.interface
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import Optional, Type, TypeVar

from lori.core import Context, Registry, ResourceException
from lori.core.activator import Activator, ActivatorMeta
from lori.core.configs import ConfigurationException, Configurations


# noinspection PyShadowingBuiltins
def register_interface_type(
    type: str,
    *alias: str,
    factory: Callable[[Context, Configurations], InterfaceType] = None,
    replace: bool = False,
) -> Callable[[Type[InterfaceType]], Type[InterfaceType]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[InterfaceType]) -> Type[InterfaceType]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


class InterfaceMeta(ActivatorMeta):
    def __call__(cls, context: Context, configs: Configurations, **kwargs) -> InterfaceType:
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


class Interface(Activator, metaclass=InterfaceMeta):
    SECTION: str = "interface"

    __context: Context

    def __init__(self, context: Context, configs: Configurations, **kwargs) -> None:
        super().__init__(configs, **kwargs)
        self.__context = self._assert_context(context)

    @classmethod
    def _assert_context(cls, context: Context) -> Context:
        from lori.application import Application

        if context is None or not isinstance(context, Application):
            raise ResourceException(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    @classmethod
    def _assert_configs(cls, configs: Optional[Configurations]) -> Optional[Configurations]:
        if configs is None:
            raise ConfigurationException(f"Invalid '{cls.__name__}' configurations: {type(configs)}")
        return super()._assert_configs(configs)

    @property
    def context(self) -> Context:
        return self.__context

    @abstractmethod
    def start(self, *args, **kwargs) -> None:
        pass


class InterfaceException(ResourceException):
    """
    Raise if an error occurred accessing the interface.

    """


InterfaceType = TypeVar("InterfaceType", bound=Interface)

_instance: Optional[InterfaceType] = None
registry = Registry[Interface]()
