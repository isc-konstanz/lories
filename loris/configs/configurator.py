# -*- coding: utf-8 -*-
"""
loris.configs.configurable
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from abc import ABC, ABCMeta, abstractmethod
from functools import wraps
from typing import Collection, Optional

from loris import LocalResourceException
from loris.configs import ConfigurationException, Configurations
from loris.util import get_variables


class ConfiguratorMeta(ABCMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        configurator = super().__call__(*args, **kwargs)

        configurator._Configurator__configure = configurator.configure
        configurator.configure = configurator._do_configure

        return configurator


class Configurator(ABC, metaclass=ConfiguratorMeta):
    __configs: Configurations

    _configured: bool = False

    def __init__(self, configs: Optional[Configurations] = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._logger = logging.getLogger(self.__module__)
        self.__configs = configs

    def __repr__(self) -> str:
        representation = f"{type(self).__name__}: \n"
        for attr in dir(self):
            if attr.startswith("_") or attr.isupper():
                continue
            try:
                value = getattr(self, attr)
                if value is None or callable(value) or isinstance(value, Configurations):
                    continue
                if isinstance(value, Configurator):
                    value = value.name
                representation += f"\t{attr} = {value}\n"
            except (AttributeError, LocalResourceException):
                pass
        return representation + f"\tenabled = {self.is_enabled()}\n"

    @property
    def name(self) -> str:
        return type(self).__name__

    def is_enabled(self) -> bool:
        return (self.__configs is not None and
                self.__configs.enabled)

    def is_configured(self) -> bool:
        return self._configured

    @property
    def configs(self) -> Configurations:
        return self.__configs

    @abstractmethod
    def configure(self, configs: Configurations) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(configure, updated=())
    def _do_configure(self, configs: Optional[Configurations] = None) -> None:
        if configs is None:
            configs = self.__configs
        if configs is None or not configs.enabled:
            raise ConfigurationException(f"Trying to configure disabled {type(self).__name__}: {configs.name}")

        if self.is_configured():
            self._logger.warning(f"{type(self).__name__} '{configs.name}' already configured")
            return

        self._logger.debug(f"Configuring {type(self).__name__}: {configs.name}")

        self.__configure(configs)
        self._do_configure_members(get_variables(self, Configurator).values())
        self._on_configure(configs)
        if self.__configs != configs:
            self.__configs = configs
        self._configured = True

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(f"Configured {self}")

    def _do_configure_members(self, configurators: Collection[Configurator]) -> None:
        for configurator in configurators:
            if not configurator.is_enabled():
                self._logger.debug(
                    f"Skipping configuring disabled {type(configurator).__name__}: " f"{configurator.configs.name}"
                )
                continue
            configurator._do_configure()

    def _on_configure(self, configs: Configurations) -> None:
        pass
