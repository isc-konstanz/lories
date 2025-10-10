# -*- coding: utf-8 -*-
"""
lori.core.configs.configurator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
from abc import ABCMeta
from collections import OrderedDict
from functools import wraps
from logging import Logger
from typing import Any, Dict, Optional

from lori._core import _Context, _Entity  # noqa
from lori._core._configurations import Configurations, _Configurations  # noqa
from lori._core._configurator import Configurator as ConfiguratorType  # noqa
from lori._core._configurator import _Configurator  # noqa
from lori.core.configs.errors import ConfigurationError
from lori.util import get_members


class ConfiguratorMeta(ABCMeta):
    def __call__(cls, *args, **kwargs):
        configurator = super().__call__(*args, **kwargs)
        cls._wrap_method(configurator, "duplicate")
        cls._wrap_method(configurator, "configure")
        cls._wrap_method(configurator, "update")

        return configurator

    # noinspection PyShadowingBuiltins
    @staticmethod
    def _wrap_method(object: Any, method: str) -> None:
        _wrap_method = getattr(object, f"_do_{method}")
        _run_method = getattr(object, method)
        setattr(object, f"_run_{method}", _run_method)
        setattr(object, method, _wrap_method)


class Configurator(_Configurator, metaclass=ConfiguratorMeta):
    __configs: _Configurations

    _configured: bool = False
    _logger: Logger

    def __init__(
        self,
        configs: Optional[Configurations] = None,
        logger: Optional[Logger] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if logger is None:
            logger = self._build_logger()
        self._logger = logger
        self.__configs = self._assert_configs(configs)

    def __eq__(self, other: Any) -> bool:
        return self is other

    def __hash__(self) -> int:
        return hash(id(self))

    def __copy__(self) -> ConfiguratorType:
        return self.copy()

    def __replace__(self, **changes) -> ConfiguratorType:
        return self.duplicate(**changes)

    @classmethod
    def _assert_configs(cls, configs: Optional[Configurations]) -> Optional[Configurations]:
        if configs is None:
            return None
        if not isinstance(configs, _Configurations):
            raise ConfigurationError(f"Invalid '{cls.__name__}' configurations: {type(configs)}")
        return configs

    @classmethod
    def _build_logger(cls) -> Logger:
        return logging.getLogger(cls.__module__)

    def _get_vars(self) -> Dict[str, Any]:
        def _is_var(attr: str, var: Any) -> bool:
            return not (
                attr.startswith("_")
                or attr.isupper()
                or callable(var)
                or isinstance(var, _Context)
                or isinstance(var, _Configurations)
            )

        return get_members(self, filter=_is_var)

    # noinspection PyShadowingBuiltins
    def _convert_vars(self, convert: callable = str) -> Dict[str, str]:
        def _convert(var: Any) -> str:
            return str(var) if not isinstance(var, (_Context, _Configurator, _Entity)) else convert(var)

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

    # noinspection PyUnresolvedReferences
    @wraps(_Configurator.configure, updated=())
    def _do_configure(self, configs: Configurations, *args, **kwargs) -> None:
        if configs is None:
            raise ConfigurationError(f"Invalid NoneType configuration for {type(self).__name__}: {self.name}")
        if not configs.enabled:
            raise ConfigurationError(f"Trying to configure disabled {type(self).__name__}: {configs.name}")
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

    # noinspection PyUnresolvedReferences
    def update(self, configs: Configurations) -> None:
        self._run_configure(configs)

    # noinspection PyUnresolvedReferences
    @wraps(_Configurator.update, updated=())
    def _do_update(self, configs: Configurations, *args, **kwargs) -> None:
        if configs is None:
            raise ConfigurationError(f"Invalid NoneType configuration for {type(self).__name__}: {self.name}")
        if not configs.enabled:
            raise ConfigurationError(f"Trying to update disabled {type(self).__name__}: {configs.name}")
        if not self.is_configured():
            self._logger.warning(f"Trying to update unconfigured {type(self).__name__}: '{configs.path}'")
            return

        self._assert_configs(configs)
        self._at_update(configs)
        self._run_update(configs, *args, **kwargs)
        self._on_update(configs)
        self.__configs = configs

    def _at_update(self, configs: Configurations) -> None:
        pass

    def _on_update(self, configs: Configurations) -> None:
        pass

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def copy(self) -> ConfiguratorType:
        try:
            copier = super().copy
        except AttributeError:
            copier = self.duplicate
        return copier()

    # noinspection PyUnresolvedReferences
    def duplicate(self, configs: Optional[Configurations] = None, **changes) -> ConfiguratorType:
        if configs is None:
            configs = self.__configs.copy()
        try:
            duplicator = super().duplicate
        except AttributeError:
            duplicator = type(self)
        return duplicator(configs=configs, **changes)

    # noinspection PyUnresolvedReferences
    @wraps(_Configurator.duplicate, updated=())
    def _do_duplicate(self, configs: Configurations, **changes) -> ConfiguratorType:
        if configs is None:
            configs = self.__configs.copy()
        self._at_duplicate(configs=configs, **changes)

        duplicate = self._run_duplicate(configs=configs, **changes)
        if configs.enabled:
            duplicate.configure(configs)
        self._on_duplicate(duplicate)
        return duplicate

    def _at_duplicate(self, **changes) -> None:
        pass

    def _on_duplicate(self, configurator: Configurator) -> None:
        pass
