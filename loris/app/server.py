# -*- coding: utf-8 -*-
"""
loris.app.server
~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Type

from loris.core import Context, ResourceException, ResourceUnavailableException
from loris.core.configs import ConfigurationException, Configurations, Configurator, ConfiguratorMeta


class ServerMeta(ConfiguratorMeta):
    # noinspection PyProtectedMember
    def __call__(cls, context: Context, configs: Configurations) -> Optional[Server]:
        # try:
        _cls = cls._get_class(configs)
        if cls != _cls:
            return _cls(context, configs)

        return super().__call__(context, configs)
        # except ImportError as e:
        #     # Return None
        #     pass

    # noinspection PyMethodMayBeStatic, PyUnusedLocal
    def _get_class(self, configs: Configurations) -> Type[Server]:
        from loris.app.view import ViewServer
        return ViewServer


class Server(Configurator, metaclass=ServerMeta):

    def __init__(self, context: Context, configs: Configurations, *args, **kwargs) -> None:
        from loris.app import Application
        if context is None or not isinstance(context, Application):
            raise ResourceException(f"Invalid server context '{context}': {type(context)}")
        if configs is None:
            raise ConfigurationException("Missing configuration")
        super().__init__(context, configs, *args, **kwargs)

    @abstractmethod
    def start(self) -> None:
        pass


class ServerException(ResourceException):
    """
    Raise if an error occurred accessing the server.

    """


class ServerUnavailableException(ResourceUnavailableException, ServerException):
    """
    Raise if a configured server access can not be found.

    """
