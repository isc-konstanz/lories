# -*- coding: utf-8 -*-
"""
lori.data.channels.converter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict

from lori.core import ResourceException


class ChannelConverter:
    __configs: OrderedDict[str, Any]

    # noinspection PyShadowingBuiltins
    def __init__(self, converter, configs: Dict[str, Any] = ()) -> None:
        self.__configs = OrderedDict(configs)
        self._converter = self._assert_converter(converter)

        self.enabled = self.__configs.pop("enabled", converter is not None)

    # noinspection PyMethodMayBeStatic
    def _assert_converter(self, converter):
        from lori.converters import Converter

        if converter is None or not isinstance(converter, Converter):
            raise ResourceException(f"Invalid converter: {None if converter is None else type(converter)}")
        return converter

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.key})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\tid={self.id}\n\t" + "\n\t".join(
            f"{k}={v}" for k, v in self._get_vars().items()
        )

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = ChannelConverter.__getattribute__(self, f"_{ChannelConverter.__name__}__configs")
        if attr in configs.keys():
            return configs[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no configuration '{attr}'")

    def __call__(self, value: Any) -> Any:
        return self._converter.convert(value)

    @property
    def id(self) -> str:
        return self._converter.id

    @property
    def key(self) -> str:
        return self._converter.key

    def copy(self) -> ChannelConverter:
        configs = self._copy_configs()
        configs["enabled"] = self.enabled
        return type(self)(self._converter, configs)

    # noinspection PyShadowingBuiltins
    def _copy_configs(self) -> Dict[str, Any]:
        return OrderedDict(**self.__configs)

    # noinspection PyShadowingBuiltins
    def _get_vars(self) -> Dict[str, Any]:
        vars = self._copy_configs()
        vars["enabled"] = self.enabled
        return vars
