# -*- coding: utf-8 -*-
"""
loris.core.configurable
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from abc import ABC, ABCMeta
from functools import wraps
from typing import Any, Dict, List, Optional

from loris.core import Context
from loris.core.configs import ConfigurationException, Configurations
from loris.util import get_members


class ConfiguratorMeta(ABCMeta):
    # noinspection PyProtectedMember
    def __call__(cls, *args, **kwargs):
        configurator = super().__call__(*args, **kwargs)

        configurator._Configurator__configure = configurator.configure
        configurator.configure = configurator._do_configure

        return configurator


class Configurator(ABC, object, metaclass=ConfiguratorMeta):
    __context: Context
    __configs: Configurations

    _configured: bool = False

    def __init__(
        self,
        context: Optional[Context] = None,
        configs: Optional[Configurations] = None,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self._logger = logging.getLogger(self.__module__)
        self.__configs = configs
        self.__context = context

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
        values = [f"{k}={v if not isinstance(type(v), type) else parse(v)}"
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
    def context(self) -> Optional[Context]:
        return self.__context

    def is_enabled(self) -> bool:
        return (self.__configs is not None and
                self.__configs.enabled)

    def is_configured(self) -> bool:
        return self._configured

    @property
    def configs(self) -> Optional[Configurations]:
        return self.__configs

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
            self._logger.warning(f"{type(self).__name__} '{configs.path}' already configured")
            return

        self.__configure(configs)
        self._on_configure(configs)
        if self.__configs != configs:
            self.__configs = configs
        self._configured = True

    def _on_configure(self, configs: Configurations) -> None:
        pass
