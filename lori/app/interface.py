# -*- coding: utf-8 -*-
"""
lori.app.interface
~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Optional, Type

from lori.core import Context, ResourceException, ResourceUnavailableException
from lori.core.configs import ConfigurationException, Configurations, Configurator, ConfiguratorMeta


class InterfaceMeta(ConfiguratorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, context: Context, configs: Configurations) -> Optional[Interface]:
        _cls = cls
        try:
            _cls = cls._get_class(configs)

        except ImportError as e:
            print(e)
            # TODO: Find better way to differentiate between interface model to use
            pass

        if cls != _cls:
            return _cls(context, configs)
        return super().__call__(context, configs)

    # noinspection PyMethodMayBeStatic, PyUnusedLocal
    def _get_class(self, configs: Configurations) -> Type[Interface]:
        from lori.app.view import ViewInterface

        return ViewInterface


class Interface(Configurator, metaclass=InterfaceMeta):
    SECTION: str = "interface"

    def __init__(self, context: Context, configs: Configurations, *args, **kwargs) -> None:
        from lori.app import Application

        if context is None or not isinstance(context, Application):
            raise ResourceException(f"Invalid server context '{context}': {type(context)}")
        if configs is None:
            raise ConfigurationException("Missing configuration")
        super().__init__(context, configs, *args, **kwargs)

    def start(self) -> None:
        pass


class InterfaceException(ResourceException):
    """
    Raise if an error occurred accessing the interface.

    """


class InterfaceUnavailableException(ResourceUnavailableException, InterfaceException):
    """
    Raise if a configured interface access can not be found.

    """
