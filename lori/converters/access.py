# -*- coding: utf-8 -*-
"""
lori.converters.access
~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Collection, Optional, TypeVar

from lori.converters import Converter
from lori.core import Directory, Registrator, RegistratorAccess, RegistratorContext, ResourceException
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
        configs_file: Optional[str] = None,
        configs_dir: Optional[str | Directory] = None,
        **kwargs: Any,
    ) -> Collection[C]:
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
            includes=Converter.INCLUDES,
            **kwargs,
        )
