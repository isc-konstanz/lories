# -*- coding: utf-8 -*-
"""
lori.core.register.context
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import itertools
import os
import re
from abc import abstractmethod
from copy import deepcopy
from typing import Any, Collection, Mapping, Optional, Sequence, TypeVar, get_args

from lori.core import Configurations, Configurator, Context, Directories, ResourceException
from lori.core.register import Registrator, Registry
from lori.util import validate_key

R = TypeVar("R", bound=Registrator)


class RegistratorContext(Context[R], Configurator):
    __context: Context

    @property
    @abstractmethod
    def _registry(self) -> Registry[R]:
        pass

    def __init__(
        self,
        context: Context,
        configs: Optional[Configurations] = None,
        **kwargs,
    ) -> None:
        super().__init__(configs=configs, **kwargs)
        self.__context = self._assert_context(context)

    @classmethod
    def _assert_context(cls, context: Context) -> Context:
        from lori.data.manager import DataManager

        if context is None or not isinstance(context, DataManager):
            raise ResourceException(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    # noinspection PyProtectedMember, PyUnresolvedReferences
    def _load_sections(
        self,
        context: RegistratorContext | Registrator,
        configs: Configurations,
        defaults: Optional[Mapping[str, Any]] = None,
        default_sections: Optional[Sequence[str]] = (),
    ) -> Collection[R]:
        values = []
        if defaults is None:
            defaults = {}
        for section in default_sections:
            if section in configs:
                defaults.update(configs.get(section))
        for section_name in configs.sections:
            if section_name in default_sections:
                continue
            section_file = f"{section_name}.conf"
            section_default = deepcopy(defaults)
            section_default.update(configs.get(section_name))
            section = Configurations.load(
                section_file,
                **configs.dirs.to_dict(),
                **section_default,
                require=False,
            )
            values.append(self._update(context, section))
        return values

    # noinspection PyProtectedMember, PyTypeChecker, PyUnresolvedReferences
    @staticmethod
    def _load_from_file(
        context: RegistratorContext,
        configs_dirs: Directories,
        configs_file: str,
        defaults: Mapping[str, Any],
    ) -> Collection[R]:
        values = []
        if configs_dirs.conf.joinpath(configs_file).is_file():
            configs = Configurations(configs_file, deepcopy(configs_dirs))
            configs._load()
            values.extend(context._load_sections(context, configs, defaults))
        return values

    # noinspection PyTypeChecker, PyProtectedMember, PyUnresolvedReferences
    def _load_from_dir(
        self,
        context: RegistratorContext | Registrator,
        configs_dir: str,
        defaults: Mapping[str, Any],
    ) -> Collection[R]:
        values = []
        if os.path.isdir(configs_dir):
            config_types = tuple(itertools.chain(*[[t.type, *t.alias] for t in self._registry.types.values()]))
            for configs_entry in os.scandir(configs_dir):
                if (
                    configs_entry.is_file()
                    and not configs_entry.path.endswith("default.conf")
                    and configs_entry.path.endswith(".conf")
                    and configs_entry.name.startswith(config_types)
                ):
                    configs_dirs = self.configs.dirs.to_dict()
                    configs_dirs["conf_dir"] = os.path.dirname(configs_entry.path)
                    configs = Configurations.load(
                        configs_entry.name,
                        **configs_dirs,
                        **defaults,
                    )
                    values.append(self._update(context, configs))
        return values

    @property
    def context(self) -> Context:
        return self.__context

    # noinspection PyMethodMayBeStatic
    def get_types(self) -> Collection[str]:
        return self._registry.types.keys()

    def has_type(self, *types: str | type) -> bool:
        if len(types) == 0:
            raise ValueError("At least one type to look up required")
        return len(self.get_all(*types)) > 0

    def get_all(self, *types: Optional[type]) -> Collection[R]:
        # noinspection PyUnresolvedReferences
        def _is_type(value: R) -> bool:
            if len(types) == 0:
                return True
            for _type in types:
                if isinstance(_type, str) and self._registry.has_type(_type):
                    return self._registry.types[_type].is_instance(value)
                if isinstance(_type, type) and isinstance(value, _type):
                    return True
            return False

        return [v for v in self.values() if _is_type(v)]

    def get_first(self, *types: Optional[str | type]) -> Optional[R]:
        return next(iter(self.get_all(*types)))

    # noinspection PyTypeChecker
    def get_last(self, *types: Optional[str | type]) -> Optional[R]:
        return next(reversed(self.get_all(*types)))

    # noinspection PyUnresolvedReferences, PyProtectedMember, SpellCheckingInspection
    def _new(self, context: Context, configs: Configurations) -> R:
        registration_class = get_args(self._registry.__orig_class__)[0]
        registration_path = "_".join(os.path.splitext(configs.name)[:-1])
        registration_key = validate_key(registration_path)
        registration_type = re.split(r"[^a-zA-Z0-9_]", registration_path)[0]
        registrator_section = configs.get_section(registration_class.SECTION, ensure_exists=True)
        if "type" in registrator_section:
            registration_type = registrator_section.get("type").lower()
        elif "type" in configs:
            _registration_type = configs.get("type").lower()
            if self._registry.has_type(_registration_type):
                registration_type = _registration_type
        if not self._registry.has_type(registration_type):
            raise ResourceException(f"Invalid registration type: {registration_type}")

        for registration in self._registry.types.values():
            if registration.is_alias(registration_type):
                registration_type = registration.type
                self._logger.debug(
                    f"Using alias \"{','.join(registration.alias)}\" " f"for registration: {registration_type}"
                )
        if "key" not in registrator_section:
            registrator_section["key"] = registration_key
            registrator_section.move_to_top("key")
        if "type" not in registrator_section:
            registrator_section["type"] = registration_type
            registrator_section.move_to_top("type")
        return self._registry.types[registration_type].initialize(context, configs)

    def _update(self, context: Context, configs: Configurations) -> R:
        # TODO: Break out ID parsing to static function, to avoid object initialization as seen here
        value = self._new(context, configs)
        if value.id in self:
            self._get(value.id).configs.update(configs)
        else:
            self._add(value)
        return value
