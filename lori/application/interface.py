# -*- coding: utf-8 -*-
"""
lori.application.interface
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Optional, Type

from lori.core import Context, ResourceException
from lori.core.configs import ConfigurationException, Configurations, Configurator, ConfiguratorMeta


class InterfaceMeta(ConfiguratorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, context: Context, configs: Configurations) -> Optional[Interface]:
        _cls = cls
        try:
            _cls = cls._get_class(configs)

        except ModuleNotFoundError:
            # TODO: Find better way to differentiate between interface model to use
            pass

        if cls != _cls:
            return _cls(context, configs)
        return super().__call__(context, configs)

    # noinspection PyMethodMayBeStatic, PyUnusedLocal
    def _get_class(self, configs: Configurations) -> Type[Interface]:
        from lori.application.view import ViewInterface

        return ViewInterface


class Interface(Configurator, metaclass=InterfaceMeta):
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

    def start(self) -> None:
        pass


class InterfaceException(ResourceException):
    """
    Raise if an error occurred accessing the interface.

    """
