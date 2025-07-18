# -*- coding: utf-8 -*-
"""
lori.core.register.access
~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Generic, TypeVar, overload

from lori.core import Configurations, Context, ResourceException
from lori.core.register import Registrator
from lori.core.register.context import RegistratorContext, _RegistratorContext
from lori.util import update_recursive

R = TypeVar("R", bound=Registrator)


# noinspection SpellCheckingInspection, PyAbstractClass, PyShadowingBuiltins
class RegistratorAccess(_RegistratorContext[R], Generic[R]):
    _registrar: Registrator
    __context: Context

    # noinspection PyProtectedMember
    def __init__(
        self,
        context: RegistratorContext,
        registrar: Registrator,
        section: str,
        **kwargs,
    ) -> None:
        context = self._assert_context(context)
        registrar = self._assert_registrar(registrar)
        super().__init__(section, logger=registrar._logger, **kwargs)
        self.__context = context
        self._registrar = registrar

    @classmethod
    def _assert_registrar(cls, registrar: Registrator):
        if registrar is None or not isinstance(registrar, Registrator):
            raise ResourceException(f"Invalid '{cls.__name__}' registrator: {type(registrar)}")
        return registrar

    @classmethod
    def _assert_context(cls, context: RegistratorContext):
        if context is None or not isinstance(context, RegistratorContext):
            raise ResourceException(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(str(c.key) for c in self.values())})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{v.key} = {repr(v)}" for v in self.values())

    # noinspection PyArgumentList
    def __getattr__(self, attr):
        registrators = Context.__getattribute__(self, f"_{Context.__name__}__map")
        registrators_by_key = {c.key: c for c in registrators.values()}
        if attr in registrators_by_key:
            return registrators_by_key[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{attr}'")

    @property
    def context(self) -> Context:
        return self.__context

    def _get_registrator_section(self) -> Configurations:
        return self._registrar.configs.get_section(self._section, ensure_exists=True)

    def __validate_id(self, id: str) -> str:
        if not len(id.split(".")) > 1:
            id = f"{self._registrar.id}.{id}"
        return id

    def _contains(self, __registrator: str | R) -> bool:
        registrators = Context.__getattribute__(self, f"_{Context.__name__}__map")
        if isinstance(__registrator, str):
            __registrator = self.__validate_id(__registrator)
            return __registrator in registrators.keys()
        if isinstance(__registrator, Registrator):
            return __registrator in registrators.values()
        return False

    def _get(self, id: str) -> R:
        return super()._get(self.__validate_id(id))

    def _set(self, id: str, registrator: R) -> None:
        id = self.__validate_id(id)

        self.context._set(id, registrator)
        super()._set(id, registrator)

    def _create(
        self,
        context: Context | Registrator,
        configs: Configurations,
        **kwargs: Any,
    ) -> R:
        return self.context._create(context, configs, **kwargs)

    def _remove(self, *__registrators: str | R) -> None:
        for __registrator in __registrators:
            if isinstance(__registrator, str):
                __registrator = self.__validate_id(__registrator)

            self.context._remove(__registrator)
            super()._remove(__registrator)

    @overload
    def add(self, key: str, **configs: Any) -> None: ...

    @overload
    def add(self, registrator: Registrator) -> None: ...

    # noinspection PyProtectedMember, PyTypeChecker
    def add(self, __registrator: str | R, **configs: Any) -> None:
        if isinstance(__registrator, Registrator):
            self._add(__registrator)
            return

        registrators = self._get_registrator_section()
        if not registrators.has_section(__registrator):
            registrators._add_section(__registrator, configs)
        else:
            registrator_configs = registrators[__registrator]
            registrator_configs = update_recursive(registrator_configs, configs, replace=False)
            registrators[__registrator] = registrator_configs

        if self._registrar.is_configured():
            registrator_configs = Configurations.load(
                f"{__registrator}.conf",
                **registrators.dirs.to_dict(),
                **self._build_defaults(registrators),
            )
            # Be wary of the order. First, update the registrator core with the default core
            # of the configuration file, then update the function arguments. Last, override
            # everything with the registrator specific configurations of the file.
            registrator_configs = update_recursive(registrator_configs, configs)
            registrator_configs = update_recursive(registrator_configs, registrators[__registrator])
            registrator = self._load_from_configs(self._registrar, registrator_configs)
            if registrator.is_enabled():
                registrator.configure(registrator_configs)

    # noinspection PyUnresolvedReferences
    def duplicate(self, configs: Configurations, **_) -> None:
        for registrator in self.values():
            relative_dir = registrator.configs.dirs.conf.relative_to(registrator.context.configs.dirs.conf)
            registrator_dirs = configs.dirs.copy()
            registrator_dirs.conf = registrator_dirs.conf.joinpath(relative_dir)
            registrator.configs.copy(registrator_dirs)
