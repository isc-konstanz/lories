# -*- coding: utf-8 -*-
"""
loris.core.configs.configurable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from abc import ABC, ABCMeta
from collections import OrderedDict
from functools import wraps
from typing import Any, Dict, Optional

from loris.core import Context, ResourceException
from loris.core.configs import ConfigurationException, Configurations
from loris.util import get_context, get_members


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
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._logger = logging.getLogger(self.__module__)
        self.__context = self._assert_context(context)
        self.__configs = self._assert_configs(configs)

    def __eq__(self, other: Any) -> bool:
        return self is other

    def __hash__(self) -> int:
        return hash(id(self))

    # noinspection PyMethodMayBeStatic
    def _assert_configs(self, configs: Optional[Configurations]) -> Optional[Configurations]:
        if configs is None:
            return None
        if not isinstance(configs, Configurations):
            raise ConfigurationException(f"Invalid configurations: {None if configs is None else type(configs)}")
        return configs

    # noinspection PyMethodMayBeStatic
    def _assert_context(self, context: Optional[Context]) -> Optional[Context]:
        if context is None:
            return None
        if not isinstance(context, Context):
            raise ResourceException(f"Invalid context: {None if context is None else type(context)}")
        return get_context(context, Context)

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
            return str(var) if not isinstance(var, (Configurator, Context)) else convert(var)

        vars = self._get_vars()
        values = OrderedDict([(k, _convert(v)) for k, v in vars.items()])
        if self.context is not None:
            values["context"] = convert(self.context)
        if self.configs is not None:
            values["configurations"] = convert(self.configs)
            values["configured"] = str(self.is_configured())
            values["enabled"] = str(self.is_enabled())
        return values

    # noinspection PyShadowingBuiltins
    def __repr__(self) -> str:
        vars = [f"{k}={v}" for k, v in self._convert_vars(lambda v: f"<{type(v).__name__}>").items()]
        return f"{type(self).__name__}({', '.join(vars)})"

    # noinspection PyShadowingBuiltins
    def __str__(self) -> str:
        vars = [f"{k} = {v}" for k, v in self._convert_vars(repr).items()]
        return f"{type(self).__name__}:\n\t" + "\n\t".join(vars)

    @property
    def context(self) -> Optional[Context]:
        return self.__context

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

        self.__configure(configs, *args, **kwargs)
        self._on_configure(configs)
        if self.__configs != configs:
            self.__configs = configs
        self._configured = True

    def _on_configure(self, configs: Configurations) -> None:
        pass
