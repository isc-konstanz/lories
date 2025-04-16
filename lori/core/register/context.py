# -*- coding: utf-8 -*-
"""
lori.core.register.context
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import os
import re
from abc import abstractmethod
from copy import deepcopy
from typing import Any, Collection, Generic, Mapping, Optional, Type, TypeVar

from lori.core import Configurations, Configurator, Context, Directories, Directory, ResourceException
from lori.core.register import RegistrationException, Registrator, Registry
from lori.util import update_recursive, validate_key

# FIXME: Remove this once Python >= 3.9 is a requirement
try:
    from typing import get_args

except ImportError:
    from typing_extensions import get_args

R = TypeVar("R", bound=Registrator)


# noinspection SpellCheckingInspection, PyAbstractClass, PyProtectedMember
class _RegistratorContext(Context[R], Generic[R]):
    _section: str

    def __init__(self, section: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._logger = logging.getLogger(self.__module__)
        self._section = section

    def _load(
        self,
        context: Context | Registrator,
        configs: Configurations,
        configs_file: Optional[str] = None,
        configs_dir: Optional[str | Directory] = None,
        includes: Optional[Collection[str]] = (),
        sort: bool = True,
        **kwargs: Any,
    ) -> Collection[R]:
        registrators = []
        defaults = Registrator._build_defaults(configs, includes)

        configs_dirs = configs.dirs.copy()
        if configs_dir is None:
            configs_dir = configs.dirs.conf.joinpath(configs.name.replace(".conf", ".d"))
        if isinstance(configs_dir, str) and not os.path.isabs(configs_dir):
            configs_dir = configs_dirs.conf.joinpath(configs_dir)
        configs_dirs.conf = configs_dir
        if configs_file is None:
            configs_file = configs.name
        configs_sections = configs.get_sections([s for s in configs.sections if s not in defaults])

        registrators.extend(self._load_from_sections(context, configs_sections, defaults, **kwargs))
        registrators.extend(self._load_from_file(context, configs_file, configs.dirs, defaults, **kwargs))
        registrators.extend(self._load_from_dir(context, configs_dirs, defaults, **kwargs))

        if sort:
            self.sort()
        return registrators

    def _load_from_configs(
        self,
        context: Context | Registrator,
        configs: Configurations,
        **kwargs: Any,
    ) -> R:
        registrator_id = Registrator._build_id(context=context, configs=configs)
        if self._contains(registrator_id):
            self._update(registrator_id, configs)
            return self._get(registrator_id)

        registrator = self._create(context, configs, **kwargs)
        self._add(registrator)
        return registrator

    def _load_from_sections(
        self,
        context: Context | Registrator,
        configs: Configurations,
        includes: Optional[Collection[str]] = (),
        defaults: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> Collection[R]:
        registrators = []
        if defaults is None:
            defaults = {}
        update_recursive(defaults, Registrator._build_defaults(configs))

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
        context: Context | Registrator,
        configs_file: str,
        configs_dirs: Directories,
        defaults: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> Collection[R]:
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
        context: Registrator,
        configs_dirs: Directories,
        defaults: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> Collection[R]:
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

                    except RegistrationException:
                        # Skip files with missing or unknown type
                        # TODO: Introduce debug logging here
                        pass
        return registrators

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

            if self._logger.level == logging.DEBUG:
                self._logger.debug(f"Configured {configurator}")

    # noinspection PyShadowingBuiltins, PyArgumentList
    def _update(self, id: str, configs: Configurations) -> None:
        registrator = self._get(id)
        if registrator.is_enabled():
            registrator.configure(configs)

    def get_all(self, *types: Type) -> Collection[R]:
        if len(types) == 0:
            return self.values()
        return self.filter(lambda r: any(isinstance(r, t) for t in types))

    def get_first(self, *types: Optional[str | Type]) -> Optional[R]:
        registrators = self.get_all(*types)
        return next(iter(registrators)) if len(registrators) > 0 else None

    # noinspection PyTypeChecker
    def get_last(self, *types: Optional[str | Type]) -> Optional[R]:
        registrators = self.get_all(*types)
        return next(reversed(registrators)) if len(registrators) > 0 else None

    def has_type(self, *types: str | Type) -> bool:
        if len(types) == 0:
            raise ValueError("At least one type to look up required")
        return len(self.get_all(*types)) > 0

    @abstractmethod
    def _get_registrator_section(self) -> Configurations:
        pass

    @abstractmethod
    def load(self, **kwargs: Any) -> Collection[R]:
        pass


# noinspection SpellCheckingInspection, PyProtectedMember
class RegistratorContext(_RegistratorContext[R], Generic[R]):
    __context: Context

    def __init__(self, context: Context, section: str, **kwargs) -> None:
        super().__init__(section, **kwargs)
        self.__context = self._assert_context(context)

    @classmethod
    def _assert_context(cls, context: Context) -> Context:
        from lori.data.manager import DataManager

        if context is None or not isinstance(context, DataManager):
            raise ResourceException(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    @property
    def context(self) -> Context:
        return self.__context

    @property
    @abstractmethod
    def _registry(self) -> Registry[R]:
        pass

    # noinspection PyUnresolvedReferences
    def _get_class(self) -> Type[R]:
        return get_args(self._registry.__orig_class__)[0]

    # noinspection PyMethodMayBeStatic
    def get_types(self) -> Collection[str]:
        return self._registry.get_types()

    def get_all(self, *types: str | Type) -> Collection[R]:
        def _is_type(regitrator) -> bool:
            for _type in types:
                if isinstance(_type, str) and self._registry.has_type(_type):
                    return self._registry.from_type(_type).is_instance(regitrator)
                if isinstance(_type, type) and isinstance(regitrator, _type):
                    return True
            return False

        if len(types) == 0:
            return self.values()
        return self.filter(_is_type)

    # noinspection PyUnresolvedReferences
    def _get_registrator_section(self) -> Configurations:
        return self.__context.configs.get_section(self._section, ensure_exists=True)

    def _create(
        self,
        context: Context | Registrator,
        configs: Configurations,
        **kwargs: Any,
    ) -> R:
        registration_class = self._get_class()
        registrator_section = configs.get_section(registration_class.SECTION, ensure_exists=True)

        if "key" not in registrator_section:
            if "key" in configs:
                registration_key = configs.get("key")
                del configs["key"]
            else:
                registration_key = "_".join(os.path.splitext(configs.name)[:-1])
            registrator_section["key"] = validate_key(registration_key)
            registrator_section.move_to_top("key")

        if "name" not in registrator_section and "name" in configs:
            registration_name = configs.get("name")
            registrator_section["name"] = registration_name
            registrator_section.move_to_top("name")
            del configs["name"]

        registration_type = re.split(r"[^a-zA-Z0-9_]", configs.name)[0]
        if "type" in registrator_section:
            registration_type = validate_key(registrator_section.get("type"))
        elif "type" in configs:
            _registration_type = validate_key(configs.get("type"))
            if self._registry.has_type(_registration_type) or not self._registry.has_type(registration_type):
                registration_type = _registration_type
                del configs["type"]

        registration = self._registry.from_type(registration_type)
        if "type" not in registrator_section:
            registrator_section["type"] = registration.key
            registrator_section.move_to_top("type")
        return registration.initialize(context, configs, **kwargs)

    @abstractmethod
    def load(self, configs: Configurations, **kwargs: Any) -> Collection[R]:
        pass
