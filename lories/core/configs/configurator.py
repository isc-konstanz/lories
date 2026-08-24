# -*- coding: utf-8 -*-
"""
lories.core.configs.configurator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import inspect
import logging
from abc import ABCMeta
from collections import OrderedDict
from functools import wraps
from logging import Logger
from typing import Any, Dict, Optional

from lories._core import _Context, _Entity  # noqa
from lories._core._configurations import Configurations, _Configurations  # noqa
from lories._core._configurator import Configurator as ConfiguratorType  # noqa
from lories._core._configurator import _Configurator  # noqa
from lories.core.configs.errors import ConfigurationError
from lories.core.configs.parameters.base import _Parameter, _TypedParameter
from lories.core.configs.parameters.entity import _EntityParameter
from lories.core.configs.parameters.group import ParameterGroup
from lories.io.shell import ANSI_KEY as _WK
from lories.io.shell import ANSI_RESET as _WR
from lories.io.shell import ANSI_WARN as _WH
from lories.util import get_members


class ConfiguratorMeta(ABCMeta):
    __config_parameters__: Dict[str, _Parameter]

    def __new__(mcls, name, bases, namespace):
        # Inherit parameters from all parent classes (order: left-to-right MRO).
        params: Dict[str, _Parameter] = {}
        for base in bases:
            parent_params = getattr(base, "__config_parameters__", {})
            params.update(parent_params)

        # Collect Parameter / ParameterGroup descriptors from this class body.
        for attr, value in namespace.items():
            if isinstance(value, _Parameter):
                # Inject the attribute name so _resolve_key() can fall back to it.
                if value.name is None:
                    value.name = attr
                params[attr] = value

        namespace["__config_parameters__"] = params
        return super().__new__(mcls, name, bases, namespace)

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

    _CONFIGS_RESERVED_KEYS: frozenset = frozenset({"enabled", "disabled", "name", "key", "id"})

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

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_logger", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._logger = self._build_logger()

    @classmethod
    def _assert_configs(cls, configs: Optional[Configurations]) -> Optional[Configurations]:
        if configs is None:
            return None
        if not isinstance(configs, _Configurations):
            raise ConfigurationError(f"Invalid '{cls.__name__}' configurations: {type(configs)}")

        for attr, param in cls.__config_parameters__.items():
            if isinstance(param, _TypedParameter) and param._has_default:
                k = param._resolve_key()
                if k not in configs:
                    configs.set(k, param.default, replace=False)

        cls._warn_undeclared_configs(configs)

        return configs

    @classmethod
    def _warn_undeclared_configs(cls, configs: _Configurations) -> None:
        logger = logging.getLogger(cls.__module__)
        if not logger.isEnabledFor(logging.DEBUG):
            return

        try:
            cls_file = inspect.getfile(cls)
        except (TypeError, OSError):
            cls_file = "<unknown file>"

        param_map = {p._resolve_key(): p for p in cls.__config_parameters__.values()}
        cls._warn_undeclared_in_section(
            configs,
            param_map,
            logger,
            cls_file,
            section_path="",
            reserved=cls._CONFIGS_RESERVED_KEYS,
        )

    @classmethod
    def _warn_undeclared_in_section(
        cls,
        configs: _Configurations,
        param_map: dict,
        logger: logging.Logger,
        cls_file: str,
        *,
        section_path: str = "",
        reserved: frozenset = frozenset(),
    ) -> None:
        declared_flat = {k for k, p in param_map.items() if isinstance(p, _TypedParameter)}
        declared_sections = {k for k, p in param_map.items() if isinstance(p, (ParameterGroup, _EntityParameter))}

        for key in configs:
            if key in reserved:
                continue

            if section_path:
                key_display = f"[{section_path}].{key}"
                section_display = f"[{section_path}.{key}]"
                child_path = f"{section_path}.{key}"
            else:
                key_display = key
                section_display = f"[{key}]"
                child_path = key

            if isinstance(configs[key], _Configurations):
                if key not in declared_sections:
                    logger.debug(
                        f"{_WH}%s (module: %s, file: %s): "
                        f"config section '{_WK}%s{_WH}' has no ParameterGroup declaration{_WR}",
                        cls.__name__,
                        cls.__module__,
                        cls_file,
                        section_display,
                    )
                    # Recurse with an empty param_map so every key inside the
                    # undeclared section is also reported individually.
                    if configs.has_member(key):
                        cls._warn_undeclared_in_section(
                            configs.get_member(key),
                            {},
                            logger,
                            cls_file,
                            section_path=child_path,
                        )
                else:
                    group = param_map[key]
                    if isinstance(group, _EntityParameter):
                        # Entity slots delegate validation to the referenced
                        # Component / Connector class itself — no recursion here.
                        continue
                    if group.children and configs.has_member(key):
                        child_map = {p._resolve_key(): p for p in group.children.values()}
                        cls._warn_undeclared_in_section(
                            configs.get_member(key),
                            child_map,
                            logger,
                            cls_file,
                            section_path=child_path,
                        )
            else:
                if key not in declared_flat and key not in declared_sections:
                    logger.debug(
                        f"{_WH}%s (module: %s, file: %s): config key '{_WK}%s{_WH}' has no Parameter declaration{_WR}",
                        cls.__name__,
                        cls.__module__,
                        cls_file,
                        key_display,
                    )

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
        self.__configs = configs  # always store the real Configurations object
        self._configured = True

    def _at_configure(self, configs: Configurations) -> None:
        """Bind declared parameters onto the instance; overrides MUST call ``super()``."""
        for attr, param in type(self).__config_parameters__.items():
            # entity slots are loaded later via Component/ConnectorAccess; skip so resolve()==None doesn't clobber
            if isinstance(param, _EntityParameter):
                continue
            setattr(self, attr, param.resolve(configs))

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
            if duplicator.__isabstractmethod__:
                duplicator = type(self)
        except AttributeError:
            duplicator = type(self)
        return duplicator(configs=configs, **changes)

    # noinspection PyUnresolvedReferences
    @wraps(_Configurator.duplicate, updated=())
    def _do_duplicate(self, configs: Optional[Configurations] = None, **changes) -> ConfiguratorType:
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
