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
        self.__logger = logging.getLogger(Configurator.__module__)
        self.__configs = configs

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        return get_members(self, filter=lambda attr, var: not (
            attr.startswith("_") or
            attr.isupper() or
            callable(var) or
            isinstance(var, Context) or
            isinstance(var, Configurations))
        )

    # noinspection PyShadowingBuiltins
    def _parse_vars(self, vars: Optional[Dict[str, Any]] = None, parse: callable = str) -> List[str]:
        if vars is None:
            vars = self._get_vars()
        values = [f"{k}={v if not isinstance(v, (Channel, Configurator, Location)) else parse(v)}"
                  for k, v in vars.items()]

        if self.context is not None:
            values.append(f"context={parse(self.context)}")
        if self.configs is not None:
            values.append(f"configurations={repr(self.configs)}")
            values.append(f"configured={self.is_configured()}")
            values.append(f"enabled={self.is_enabled()}")
        return values

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        return get_members(self, filter=lambda attr, var: not (
            attr.startswith("_") or
            attr.isupper() or
            callable(var) or
            isinstance(var, Context) or
            isinstance(var, Configurations))
        )

    # noinspection PyShadowingBuiltins
    def _parse_vars(self, vars: Optional[Dict[str, Any]] = None, parse: callable = str) -> List[str]:
        if vars is None:
            vars = self._get_vars()
        values = [f"{k}={v if not isinstance(v, (Channel, Configurator, Location)) else parse(v)}"
                  for k, v in vars.items()]

        if self.context is not None:
            values.append(f"context={parse(self.context)}")
        if self.configs is not None:
            values.append(f"configurations={repr(self.configs)}")
            values.append(f"configured={self.is_configured()}")
            values.append(f"enabled={self.is_enabled()}")
        return values

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(self._parse_vars(parse=lambda v: type(v).__name__))})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(self._parse_vars(parse=repr))

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

        self.__logger.debug(f"Configuring {type(self).__name__}: {configs.name}")

        self.__configure(configs)
        self._do_configure_members(get_variables(self, Configurator).values())
        self._on_configure(configs)
        if self.__configs != configs:
            self.__configs = configs
        self._configured = True

        if self._logger.isEnabledFor(logging.DEBUG):
            self.__logger.debug(f"Configured {self}")

    def _do_configure_members(self, configurators: Collection[Configurator]) -> None:
        for configurator in configurators:
            if not configurator.is_enabled():
                self.__logger.debug(
                    f"Skipping configuring disabled {type(configurator).__name__}: " f"{configurator.configs.name}"
                )
                continue
            configurator._do_configure()

    def _on_configure(self, configs: Configurations) -> None:
        pass
