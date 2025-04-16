# -*- coding: utf-8 -*-
"""
lori.core.configs.configurable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from abc import ABC, ABCMeta
from collections import OrderedDict
from functools import wraps
from logging import Logger
from typing import Any, Dict, Optional

from lori.core import Context, Entity
from lori.core.configs import ConfigurationException, Configurations
from lori.util import get_members


class ConfiguratorMeta(ABCMeta):
    def __call__(cls, *args, **kwargs):
        configurator = super().__call__(*args, **kwargs)
        cls._wrap_method(configurator, "configure")

        return configurator

    # noinspection PyShadowingBuiltins
    @staticmethod
    def _wrap_method(object: Any, method: str) -> None:
        setattr(object, f"_run_{method}", getattr(object, method))
        setattr(object, method, getattr(object, f"_do_{method}"))


class Configurator(ABC, object, metaclass=ConfiguratorMeta):
    __configs: Configurations

    _configured: bool = False
    _logger: Logger

    def __init__(self, configs: Optional[Configurations] = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.__configs = self._assert_configs(configs)
        self._logger = logging.getLogger(self.__module__)

    def __eq__(self, other: Any) -> bool:
        return self is other

    def __hash__(self) -> int:
        return hash(id(self))

    @classmethod
    def _assert_configs(cls, configs: Optional[Configurations]) -> Optional[Configurations]:
        if configs is None:
            return None
        if not isinstance(configs, Configurations):
            raise ConfigurationException(f"Invalid '{cls.__name__}' configurations: {type(configs)}")
        return configs

    def _get_vars(self) -> Dict[str, Any]:
        def _is_var(attr: str, var: Any) -> bool:
            return not (
                attr.startswith("_")
                or attr.isupper()
                or callable(var)
                or isinstance(var, Context)
                or isinstance(var, Configurations)
            )

        return get_members(self, filter=_is_var)

    # noinspection PyShadowingBuiltins
    def _convert_vars(self, convert: callable = str) -> Dict[str, str]:
        def _convert(var: Any) -> str:
            return str(var) if not isinstance(var, (Context, Configurator, Entity)) else convert(var)

        vars = self._get_vars()
        values = OrderedDict([(k, _convert(v)) for k, v in vars.items()])
        if self.configs is not None:
            values["enabled"] = str(self.is_enabled())
            values["configured"] = str(self.is_configured())
            values["configs"] = convert(self.configs)
        return values

    # noinspection PyShadowingBuiltins
    def __repr__(self) -> str:
        vars = [f"{k}={v}" for k, v in self._convert_vars(lambda v: f"<{type(v).__name__}>").items()]
        return f"{type(self).__name__}({', '.join(vars)})"

    # noinspection PyShadowingBuiltins
    def __str__(self) -> str:
        vars = [f"{k} = {v}" for k, v in self._convert_vars(repr).items()]
        return f"{type(self).__name__}:\n\t" + "\n\t".join(vars)

    def is_enabled(self) -> bool:
        return self.__configs is not None and self.__configs.enabled

    def is_configured(self) -> bool:
        return self._configured

    @property
    def configs(self) -> Optional[Configurations]:
        return self.__configs

    def configure(self, configs: Configurations) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @wraps(configure, updated=())
    def _do_configure(self, configs: Configurations, *args, **kwargs) -> None:
        if configs is None:
            raise ConfigurationException(f"Invalid NoneType configuration for {type(self).__name__}: {self.name}")
        if not configs.enabled:
            raise ConfigurationException(f"Trying to configure disabled {type(self).__name__}: {configs.name}")

        if self.is_configured():
            self._logger.warning(f"{type(self).__name__} '{configs.path}' already configured")
            return

        self._assert_configs(configs)
        self._at_configure(configs)
        self._run_configure(configs, *args, **kwargs)
        self._on_configure(configs)
        self.__configs = configs
        self._configured = True

    def _at_configure(self, configs: Configurations) -> None:
        pass

    def _on_configure(self, configs: Configurations) -> None:
        pass
