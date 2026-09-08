# -*- coding: utf-8 -*-
"""
lories.data.converters.context
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Callable, Collection, List, Optional, Type

from lories._core._converter import _ConverterContext  # noqa
from lories.core import Configurations, ResourceError
from lories.core.register import RegistratorContext, Registry
from lories.data.converters.converter import (
    BoolConverter,
    BytesConverter,
    Converter,
    DatetimeConverter,
    FloatConverter,
    IntConverter,
    ListConverter,
    StringConverter,
    TimestampConverter,
)
from lories.data.converters.linear import LinearConverter

CONVERTERS_BUILTINS = [
    DatetimeConverter,
    TimestampConverter,
    StringConverter,
    FloatConverter,
    IntConverter,
    BoolConverter,
    BytesConverter,
    ListConverter,
]

# Converters that always exist as one instance, keyed by name rather than by dtype.
CONVERTERS_DEFAULTS = {
    "linear": LinearConverter,
}

registry = Registry[Converter]()
registry.register(DatetimeConverter, "datetime")
registry.register(TimestampConverter, "timestamp")
registry.register(StringConverter, "str", "string")
registry.register(FloatConverter, "float")
registry.register(IntConverter, "int", "integer")
registry.register(BoolConverter, "bool", "boolean")
registry.register(BytesConverter, "byte", "bytes")
registry.register(ListConverter, "list")
registry.register(LinearConverter, "linear")


def register_converter_type(
    key: str,
    *alias: str,
    factory: Callable[[ConverterContext, Optional[Configurations]], Converter] = None,
    replace: bool = False,
) -> Callable[[Type[Converter]], Type[Converter]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[Converter]) -> Type[Converter]:
        registry.register(cls, key, *alias, factory=factory, replace=replace)
        return cls

    return _register


class ConverterContext(_ConverterContext, RegistratorContext[Converter]):
    @property
    def _registry(self) -> Registry[Converter]:
        return registry

    # noinspection PyProtectedMember
    def load(
        self,
        configs: Optional[Configurations] = None,
        configure: bool = True,
        **kwargs,
    ) -> Collection[Converter]:
        if configs is None:
            configs = self._load_registrators_configs()
        defaults = Converter._build_defaults(configs)

        converters = []
        converter_dirs = configs.dirs.to_dict()
        converter_dirs["conf_dir"] = str(configs.dirs.conf.joinpath("converters.d"))
        converter_generics = [c.type for c in registry.filter(lambda r: r.type in CONVERTERS_BUILTINS)]
        converter_defaults = [(c.dtype.__name__.lower(), c) for c in converter_generics]
        converter_defaults.extend(CONVERTERS_DEFAULTS.items())
        for converter_key, converter_cls in converter_defaults:
            converter_configs = Configurations.load(
                f"{converter_key}.conf",
                require=False,
                **converter_dirs,
                **defaults,
            )
            converter = converter_cls(context=self, configs=converter_configs, key=converter_key)
            converters.append(converter)
            self._add(converter)

        converters.extend(self._load(self, configs, includes=Converter.INCLUDES, configure=False, **kwargs))
        if configure:
            self.configure(converters)
        return converters

    def has_dtype(self, *dtypes: Type) -> bool:
        return len(self._get_by_dtypes(*dtypes)) > 0

    def get_by_dtype(self, dtype: Type) -> Converter:
        converters = self._get_by_dtypes(dtype)
        if len(converters) == 0:
            raise ResourceError(f"Converter instance for '{dtype}' does not exist")
        return converters[0]

    def _get_by_dtypes(self, dtype: Type) -> List[Converter]:
        # Only the generic dtype converters serve as a channel's default; `linear` and any named
        # instance share a dtype with them but must be selected explicitly.
        return [t for t in self.values() if type(t) in CONVERTERS_BUILTINS and issubclass(t.dtype, dtype)]
