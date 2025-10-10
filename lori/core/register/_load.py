# -*- coding: utf-8 -*-
"""
lori.core.register._load
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import os
from abc import abstractmethod
from copy import deepcopy
from logging import Logger
from typing import Any, Collection, Dict, Generic, Mapping, Optional, Sequence, Type

from lori._core._configurator import Configurator  # noqa
from lori._core._registrator import (  # noqa
    Registrator,
    RegistratorContext,
    _RegistratorContext,  # noqa
)
from lori.core.configs import Configurations, Directories, Directory
from lori.core.errors import ResourceError
from lori.util import update_recursive

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import get_args, get_origin

except ImportError:
    from typing_extensions import get_args, get_origin

# FIXME: Remove this once Python >= 3.12 is a requirement
try:
    from typing import get_bound

except ImportError:

    def get_bound(cls):
        return getattr(cls, "__bound__", None)


# noinspection SpellCheckingInspection, PyAbstractClass, PyProtectedMember
class _RegistratorLoader(_RegistratorContext[Registrator], Generic[Registrator]):
    __registrator_class: Type[Registrator]
    _logger: Logger

    def __init__(
        self,
        logger: Optional[Logger] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        if logger is None:
            logger = self._build_logger()
        self._logger = logger
        self.__registrator_class = self._build_registrators_class()

    @classmethod
    def _build_logger(cls) -> Logger:
        return logging.getLogger(cls.__module__)

    @classmethod
    def _build_registrators_class(cls) -> Type[Registrator]:
        for _cls in cls.mro():
            for _base in getattr(_cls, "__orig_bases__", ()):
                if isinstance(_base, type):
                    continue
                if issubclass(get_origin(_base), _RegistratorContext):
                    (_arg,) = get_args(_base)
                    if not isinstance(_arg, type):
                        _arg = get_bound(_arg)
                    return _arg
        raise ResourceError("Unable to extract context registrator")

    # noinspection PyUnresolvedReferences
    def _build_registrator_id(
        self,
        context: RegistratorContext,
        configs: Configurations,
    ) -> str:
        return self._get_registator_class()._build_id(context=context, configs=configs)

    # noinspection PyUnresolvedReferences
    def _build_registrator_defaults(
        self,
        configs: Configurations,
        includes: Optional[Collection[str]] = (),
    ) -> Dict[str, Any]:
        return self._get_registator_class()._build_defaults(configs, includes)

    # noinspection PyUnresolvedReferences
    def _load(
        self,
        context: RegistratorContext,
        configs: Configurations,
        configs_file: Optional[str] = None,
        configs_dir: Optional[str | Directory] = None,
        includes: Optional[Collection[str]] = (),
        defaults: Optional[Mapping[str, Any]] = None,
        configure: bool = True,
        sort: bool = True,
        **kwargs: Any,
    ) -> Sequence[Registrator]:
        registrators = []
        if defaults is None:
            defaults = {}
        update_recursive(defaults, self._build_registrator_defaults(configs, includes))

        configs_dirs = configs.dirs.copy()
        if configs_dir is None:
            configs_dir = configs.dirs.conf.joinpath(configs.name.replace(".conf", ".d"))
        if isinstance(configs_dir, str) and not os.path.isabs(configs_dir):
            configs_dir = configs_dirs.conf.joinpath(configs_dir)
        configs_dirs.conf = configs_dir
        configs_sections = configs.get_sections([s for s in configs.sections if s not in defaults])

        if configs_file and configs_file != configs.name:
            registrators.extend(self._load_from_file(context, configs_file, configs.dirs, defaults, **kwargs))

        registrators.extend(self._load_from_sections(context, configs_sections, defaults, **kwargs))
        registrators.extend(self._load_from_dir(context, configs_dirs, defaults, **kwargs))

        if sort:
            self.sort()
        if configure:
            self.configure(registrators)
        return registrators

    def _load_from_configs(
        self,
        context: RegistratorContext,
        configs: Configurations,
        **kwargs: Any,
    ) -> Registrator:
        registrator_id = self._build_registrator_id(context, configs)
        if self._contains(registrator_id):
            registrator = self._update(registrator_id, configs)
        else:
            registrator = self._create(context, configs, **kwargs)
            self._add(registrator)
        return registrator

    # noinspection PyUnresolvedReferences
    def _load_from_sections(
        self,
        context: RegistratorContext,
        configs: Configurations,
        includes: Optional[Collection[str]] = (),
        defaults: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> Sequence[Registrator]:
        registrators = []
        if defaults is None:
            defaults = {}
        update_recursive(defaults, self._build_registrator_defaults(configs))

        for section_name in configs.sections:
            if section_name in includes:
                continue
            section_file = f"{section_name}.conf"
            section_default = deepcopy(defaults)
            update_recursive(section_default, configs.get(section_name))

            section = Configurations.load(
                section_file,
                **configs.dirs.to_dict(),
                **section_default,
                require=False,
            )
            registrators.append(self._load_from_configs(context, section, **kwargs))
        return registrators

    def _load_from_file(
        self,
        context: RegistratorContext,
        configs_file: str,
        configs_dirs: Directories,
        defaults: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> Sequence[Registrator]:
        registrators = []
        if configs_dirs.conf.joinpath(configs_file).is_file():
            # Do not call .load() function here, as configs_dirs._conf may be None and would otherwise be overridden
            # with the data directory
            configs = Configurations(configs_file, deepcopy(configs_dirs))
            configs._load()
            registrators.extend(self._load_from_sections(context, configs, defaults, **kwargs))
        return registrators

    def _load_from_dir(
        self,
        context: RegistratorContext,
        configs_dirs: Directories,
        defaults: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> Sequence[Registrator]:
        registrators = []
        if os.path.isdir(configs_dirs.conf):
            for configs_entry in os.scandir(configs_dirs.conf):
                if (
                    configs_entry.is_file()
                    and configs_entry.path.endswith(".conf")
                    and not configs_entry.path.endswith("default.conf")
                    and configs_entry.name
                    not in [
                        "settings.conf",
                        "system.conf",
                        "evaluations.conf",
                        "replications.conf",
                        "logging.conf",
                    ]
                ):
                    configs = Configurations.load(
                        configs_entry.name,
                        **configs_dirs.to_dict(),
                        **defaults,
                    )
                    try:
                        registrators.append(self._load_from_configs(context, configs, **kwargs))

                    except ResourceError:
                        # Skip files with missing or unknown type
                        # TODO: Introduce debug logging here
                        pass
        return registrators

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def configure(self, configurators: Optional[Collection[Configurator]] = None) -> None:
        if configurators is None:
            configurators = self.values()
        for configurator in configurators:
            configurations = configurator.configs
            if configurations is None or not configurations.enabled:
                self._logger.debug(f"Skipping configuring disabled {type(configurator).__name__}")
                continue

            self._logger.debug(f"Configuring {type(self).__name__}: {configurations.path}")
            configurator.configure(configurations)

            if self._logger.getEffectiveLevel() <= logging.DEBUG:
                self._logger.debug(f"Configured {configurator}")

    # noinspection PyUnresolvedReferences, PyArgumentList, PyShadowingBuiltins
    def _update(self, id: str, configs: Configurations) -> Registrator:
        registrator = self._get(id)
        if registrator.is_configured() and configs.enabled:
            registrator_configs = registrator.configs.copy()
            registrator_configs.update(configs)
            registrator.update(registrator_configs)
        else:
            registrator.configs.update(configs)
        return registrator

    # noinspection PyUnresolvedReferences
    def get_all(self, *types: Type[Registrator]) -> Collection[Registrator]:
        if len(types) == 0:
            return self.values()
        return self.filter(lambda r: any(isinstance(r, t) and r.is_enabled() for t in types))

    def get_first(self, *types: Optional[str | Type[Registrator]]) -> Optional[Registrator]:
        registrators = self.get_all(*types)
        return next(iter(registrators)) if len(registrators) > 0 else None

    # noinspection PyTypeChecker
    def get_last(self, *types: Optional[str | Type[Registrator]]) -> Optional[Registrator]:
        registrators = self.get_all(*types)
        return next(reversed(registrators)) if len(registrators) > 0 else None

    def has_type(self, *types: str | Type[Registrator]) -> bool:
        if len(types) == 0:
            raise ValueError("At least one type to look up required")
        return len(self.get_all(*types)) > 0

    # noinspection PyTypeChecker, PyUnresolvedReferences
    def _get_registator_class(self) -> Type[Registrator]:
        return self.__registrator_class

    @classmethod
    def _get_registrators_type(cls) -> str:
        return cls.TYPE

    @abstractmethod
    def _load_registrators_configs(self) -> Configurations: ...

    @abstractmethod
    def load(self, **kwargs: Any) -> Sequence[Registrator]:
        pass
