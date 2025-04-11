# -*- coding: utf-8 -*-
"""
lori.converters.context
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Callable, Collection, List, Optional, Type, TypeVar

from lori import ResourceException
from lori.converters.converter import (
    BoolConverter,
    BytesConverter,
    Converter,
    DatetimeConverter,
    FloatConverter,
    IntConverter,
    StringConverter,
    TimestampConverter,
)
from lori.core import Context, Registrator, RegistratorContext, Registry
from lori.core.configs import Configurations

C = TypeVar("C", bound=Converter)

CONVERTERS_BUILTINS = [
    DatetimeConverter,
    TimestampConverter,
    StringConverter,
    FloatConverter,
    IntConverter,
    BoolConverter,
    BytesConverter,
]

registry = Registry[Converter]()
registry.register(DatetimeConverter, "datetime")
registry.register(TimestampConverter, "timestamp")
registry.register(StringConverter, "str", "string")
registry.register(FloatConverter, "float")
registry.register(IntConverter, "int", "integer")
registry.register(BoolConverter, "bool", "boolean")
registry.register(BytesConverter, "byte", "bytes")


def register_converter_type(
    key: str,
    *alias: str,
    factory: Callable[[Context | Registrator, Optional[Configurations]], C] = None,
    replace: bool = False,
) -> Callable[[Type[C]], Type[C]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[C]) -> Type[C]:
        registry.register(cls, key, *alias, factory=factory, replace=replace)
        return cls

    return _register


class ConverterContext(RegistratorContext[Converter]):
    @property
    def _registry(self) -> Registry[Converter]:
        return registry

    # noinspection PyProtectedMember
    def load(self, configs: Configurations, **kwargs: Any) -> Collection[Converter]:
        defaults = Converter._build_defaults(configs)

        converters = []
        converter_dirs = configs.dirs.to_dict()
        converter_dirs["conf_dir"] = str(configs.dirs.conf.joinpath("converters.d"))
        converter_generics = [c.type for c in registry.filter(lambda r: r.type in CONVERTERS_BUILTINS)]
        for converter_cls in converter_generics:
            converter_key = converter_cls.dtype.__name__.lower()
            converter_configs = Configurations.load(
                f"{converter_key}.conf",
                require=False,
                **converter_dirs,
                **defaults,
            )
            converter = converter_cls(context=self, configs=converter_configs, key=converter_key)
            converters.append(converter)
            self._add(converter)

        converters.extend(self._load(self, configs, includes=Converter.INCLUDES, **kwargs))
        return converters

    def has_dtype(self, *dtypes: Type) -> bool:
        return len(self._get_by_dtypes(*dtypes)) > 0

    def get_by_dtype(self, dtype: Type) -> Converter:
        converters = self._get_by_dtypes(dtype)
        if len(converters) == 0:
            raise ResourceException(f"Converter instance for '{dtype}' does not exist")
        return converters[0]

    def _get_by_dtypes(self, dtype: Type) -> List[Converter]:
        return [t for t in self.values() if issubclass(t.dtype, dtype)]
