# -*- coding: utf-8 -*-
"""
lori.converters.access
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Collection, Optional, TypeVar

from lori.converters import Converter
from lori.core import Configurations, Directory, Registrator, RegistratorAccess, RegistratorContext, ResourceException
from lori.util import get_context

C = TypeVar("C", bound=Converter)


class ConverterAccess(RegistratorAccess[C]):
    # noinspection PyUnresolvedReferences
    def __init__(self, registrar: Registrator, configs: Configurations, **kwargs) -> None:
        context = get_context(registrar, RegistratorContext).context.converters
        super().__init__(context, registrar, configs=configs, **kwargs)

    # noinspection PyShadowingBuiltins
    def _set(self, id: str, converter: C) -> None:
        if not isinstance(converter, Converter):
            raise ResourceException(f"Invalid converter type: {type(converter)}")

        super()._set(id, converter)

    def load(
        self,
        configs_file: Optional[str] = None,
        configs_dir: Optional[str | Directory] = None,
        **kwargs: Any,
    ) -> Collection[C]:
        if configs_file is None:
            configs_file = self.configs.name
        if configs_dir is None:
            configs_dir = self.configs.dirs.conf.joinpath(self.configs.name.replace(".conf", ".d"))
        return self._load(
            self._registrar,
            self.configs,
            configs_file=configs_file,
            configs_dir=configs_dir,
            includes=Converter.INCLUDES,
            **kwargs,
        )
