# -*- coding: utf-8 -*-
"""
lori.data.converters.access
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Collection, Optional, TypeVar

from lori.core import Configurations, Directory, Registrator, RegistratorAccess, RegistratorContext, ResourceException
from lori.data.converters import Converter
from lori.util import get_context

C = TypeVar("C", bound=Converter)


class ConverterAccess(RegistratorAccess[C]):
    # noinspection PyUnresolvedReferences
    def __init__(self, registrar: Registrator, **kwargs) -> None:
        context = get_context(registrar, RegistratorContext).context.converters
        super().__init__(context, registrar, "converters", **kwargs)

    # noinspection PyShadowingBuiltins
    def _set(self, id: str, converter: C) -> None:
        if not isinstance(converter, Converter):
            raise ResourceException(f"Invalid converter type: {type(converter)}")

        super()._set(id, converter)

    def load(
        self,
        configs: Optional[Configurations] = None,
        configs_file: Optional[str] = None,
        configs_dir: Optional[str | Directory] = None,
        configure: bool = False,
        **kwargs: Any,
    ) -> Collection[C]:
        if configs is None:
            configs = self._get_registrator_section()
        if configs_file is None:
            configs_file = configs.name
        if configs_dir is None:
            configs_dir = configs.dirs.conf.joinpath(configs.name.replace(".conf", ".d"))
        return self._load(
            self._registrar,
            configs=configs,
            configs_file=configs_file,
            configs_dir=configs_dir,
            configure=configure,
            includes=Converter.INCLUDES,
            **kwargs,
        )
