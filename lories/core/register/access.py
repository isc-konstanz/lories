# -*- coding: utf-8 -*-
"""
lories.core.register.access
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Dict, Generic, Optional, Sequence, overload

from lories._core._context import Context, _Context  # noqa
from lories._core._registrator import (  # noqa
    Registrator,
    RegistratorContext,
    _Registrator,  # noqa
    _RegistratorContext,  # noqa
)
from lories.core.configs import Configurations
from lories.core.errors import ResourceError
from lories.core.register._load import _RegistratorLoader
from lories.util import update_recursive


# noinspection SpellCheckingInspection, PyProtectedMember, PyAbstractClass, PyShadowingBuiltins
class RegistratorAccess(_RegistratorLoader[Registrator], Generic[Registrator]):
    _registrar: Registrator
    __context: RegistratorContext

    # noinspection PyUnresolvedReferences
    def __init__(
        self,
        context: RegistratorContext,
        registrar: Registrator,
        **kwargs,
    ) -> None:
        context = self._assert_context(context)
        registrar = self._assert_registrar(registrar)
        super().__init__(logger=registrar._logger, **kwargs)
        self.__context = context
        self._registrar = registrar

    @classmethod
    def _assert_registrar(cls, registrar: Registrator):
        if registrar is None or not isinstance(registrar, _Registrator):
            raise ResourceError(f"Invalid '{cls.__name__}' registrator: {type(registrar)}")
        return registrar

    @classmethod
    def _assert_context(cls, context: RegistratorContext):
        if context is None or not isinstance(context, _RegistratorContext):
            raise ResourceError(f"Invalid '{cls.__name__}' context: {type(context)}")
        return context

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(str(c.key) for c in self.values())})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{v.key} = {repr(v)}" for v in self.values())

    # noinspection PyArgumentList
    def __getattr__(self, attr):
        registrators = _Context.__getattribute__(self, f"{_Context.__name__}__map")
        registrators_by_key = {c.key: c for c in registrators.values()}
        if attr in registrators_by_key:
            return registrators_by_key[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{attr}'")

    @property
    def context(self) -> RegistratorContext:
        return self.__context

    def __validate_id(self, id: str) -> str:
        if not len(id.split(".")) > 1:
            id = f"{self._registrar.id}.{id}"
        return id

    # noinspection PyArgumentList
    def _contains(self, __registrator: str | Registrator) -> bool:
        registrators = _Context.__getattribute__(self, f"{_Context.__name__}__map")
        if isinstance(__registrator, str):
            __registrator = self.__validate_id(__registrator)
            return __registrator in registrators.keys()
        if isinstance(__registrator, _Registrator):
            return __registrator in registrators.values()
        return False

    def _get(self, id: str) -> Registrator:
        return super()._get(self.__validate_id(id))

    def _set(self, id: str, registrator: Registrator) -> None:
        id = self.__validate_id(id)

        self.__context._set(id, registrator)
        super()._set(id, registrator)

    def _create(
        self,
        context: Context | Registrator,
        configs: Configurations,
        **kwargs: Any,
    ) -> Registrator:
        return self.context._create(context, configs, **kwargs)

    def _remove(self, *__registrators: str | Registrator) -> None:
        for __registrator in __registrators:
            if isinstance(__registrator, str):
                __registrator = self.__validate_id(__registrator)

            self.__context._remove(__registrator)
            super()._remove(__registrator)

    @overload
    def add(self, key: str, **configs: Any) -> None: ...

    @overload
    def add(self, registrator: Registrator) -> None: ...

    # noinspection PyUnresolvedReferences, PyTypeChecker
    def add(self, __registrator: str | Registrator, **configs: Any) -> None:
        if isinstance(__registrator, _Registrator):
            self._add(__registrator)
            return

        registrators = self._load_registrators_configs()
        if not registrators.has_member(__registrator):
            registrators._add_member(__registrator, configs)
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

    def _load_registrator_defaults(self, configs: Optional[Configurations] = None, **kwargs) -> Dict[str, Any]:
        _class = self._get_registator_class()
        if configs is None:
            configs = self._registrar.configs
        return _class._build_defaults(configs, **kwargs)

    # noinspection PyUnresolvedReferences
    def _load_registrators_configs(self) -> Configurations:
        _type = self._get_registrators_type()
        return self._registrar.configs.get_member(_type, ensure_exists=True)

    # noinspection PyUnresolvedReferences, PyProtectedMember
    def load(
        self,
        configs: Optional[Configurations] = None,
        configs_file: Optional[str] = None,
        configs_dir: Optional[str] = None,
        configure: bool = False,
        **kwargs: Any,
    ) -> Sequence[Registrator]:
        _class = self._get_registator_class()

        defaults = self._load_registrator_defaults(**kwargs)
        if configs is None:
            configs = self._load_registrators_configs()
        if configs_file is None:
            configs_file = configs.name
        if configs_dir is None:
            configs_dir = configs.dirs.conf.joinpath(f"{configs.key}.d")
        return self._load(
            self._registrar,
            configs=configs,
            configs_file=configs_file,
            configs_dir=configs_dir,
            configure=configure,
            includes=_class.INCLUDES,
            defaults=defaults,
        )
