# -*- coding: utf-8 -*-
"""
lories.data.converters.access
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from lories._core._component import Component  # noqa
from lories._core._converter import Converter, _Converter, _ConverterContext  # noqa
from lories._core._data import _DataManager  # noqa
from lories.core import RegistratorAccess, ResourceError
from lories.util import get_context


class ConverterAccess(_ConverterContext, RegistratorAccess[Converter]):
    # noinspection PyUnresolvedReferences
    def __init__(self, registrar: Component, **kwargs) -> None:
        context = get_context(registrar, _DataManager).converters
        super().__init__(context, registrar, **kwargs)

    # noinspection PyProtectedMember, PyShadowingBuiltins
    def _set(self, id: str, converter: Converter) -> None:
        if not isinstance(converter, _Converter):
            raise ResourceError(f"Invalid converter type: {type(converter)}")

        super()._set(id, converter)
