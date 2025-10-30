# -*- coding: utf-8 -*-
"""
lories.core.register.context
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import os
import re
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Collection, Generic, Optional, Sequence, Type

from lories._core._configurations import Configurations  # noqa
from lories._core._data import DataContext, _DataContext, _DataManager  # noqa
from lories._core._registrator import Registrator  # noqa
from lories.core.errors import ResourceError
from lories.core.register._load import _RegistratorLoader
from lories.core.register.registry import Registry
from lories.util import validate_key


# noinspection SpellCheckingInspection, PyProtectedMember
class RegistratorContext(_RegistratorLoader[Registrator], Generic[Registrator]):
    __context: _DataContext

    # noinspection PyUnresolvedReferences
    def __init__(self, context: DataContext, **kwargs) -> None:
        context = self._assert_context(context)
        super().__init__(context._logger, **kwargs)
        self.__context = context

    @classmethod
    def _assert_context(cls, context: DataContext) -> DataContext:
        if context is None or not isinstance(context, _DataManager):
            raise ResourceError(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    @property
    def context(self) -> DataContext:
        return self.__context

    # noinspection PyUnresolvedReferences
    @property
    def _executor(self) -> ThreadPoolExecutor:
        return self.__context._executor

    @property
    @abstractmethod
    def _registry(self) -> Registry[Registrator]: ...

    # noinspection PyMethodMayBeStatic
    def get_types(self) -> Collection[str]:
        return self._registry.get_types()

    def get_all(self, *types: str | Type) -> Sequence[Registrator]:
        def _is_type(regitrator) -> bool:
            for _type in types:
                if isinstance(_type, str) and self._registry.has_type(_type):
                    return self._registry.from_type(_type).is_instance(regitrator)
                if isinstance(_type, type) and isinstance(regitrator, _type):
                    return True
            return False

        if len(types) == 0:
            return list(self.values())
        return self.filter(_is_type)

    def _create(
        self,
        context: RegistratorContext | Registrator,
        configs: Configurations,
        **kwargs: Any,
    ) -> Registrator:
        registration_class = self._get_registator_class()
        registrator_member = configs.get_member(registration_class.TYPE, ensure_exists=True)

        if "key" not in registrator_member:
            if "key" in configs:
                registration_key = configs.get("key")
                del configs["key"]
            else:
                registration_key = "_".join(re.split(r"[^\w-]", configs.key))
            registrator_member["key"] = validate_key(registration_key)
            registrator_member.move_to_top("key")

        if "name" not in registrator_member and "name" in configs:
            registration_name = configs.get("name")
            registrator_member["name"] = registration_name
            registrator_member.move_to_top("name")
            del configs["name"]

        registration_type = re.split(r"[^\w_-]", configs.key)[0]
        if "type" in registrator_member:
            registration_type = validate_key(registrator_member.get("type"))
        elif "type" in configs:
            _registration_type = validate_key(configs.get("type"))
            if self._registry.has_type(_registration_type) or not self._registry.has_type(registration_type):
                registration_type = _registration_type
                del configs["type"]

        registration = self._registry.from_type(registration_type)
        if "type" not in registrator_member:
            registrator_member["type"] = registration.key
            registrator_member.move_to_top("type")
        return registration.initialize(context, configs, **kwargs)

    # noinspection PyUnresolvedReferences
    def _load_registrators_configs(self) -> Configurations:
        _type = self._get_registrators_type()
        return self.__context.configs.get_member(_type, ensure_exists=True)

    def load(self, configs: Optional[Configurations] = None, **kwargs) -> Sequence[Registrator]:
        _class = self._get_registator_class()
        if configs is None:
            configs = self._load_registrators_configs()
        return self._load(self, configs, includes=_class.INCLUDES, **kwargs)
