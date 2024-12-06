# -*- coding: utf-8 -*-
"""
lori.converters.context
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Callable, List, Optional, Type, TypeVar

from lori import ResourceException
from lori.converters.converter import (
    BoolConverter,
    Converter,
    DatetimeConverter,
    FloatConverter,
    GenericConverter,
    IntConverter,
    StringConverter,
    TimestampConverter,
)
from lori.core import Context, Registrator, RegistratorContext, Registry
from lori.core.configs import Configurations, Configurator

C = TypeVar("C", bound=Converter)

registry = Registry[Converter]()
registry.register(DatetimeConverter, "datetime")
registry.register(TimestampConverter, "timestamp")
registry.register(StringConverter, "str", "string")
registry.register(FloatConverter, "float")
registry.register(IntConverter, "int", "integer")
registry.register(BoolConverter, "bool", "boolean")


# noinspection PyShadowingBuiltins
def register_converter_type(
    type: str,
    *alias: str,
    factory: Callable[[Registrator | Context, Optional[Configurations]], C] = None,
    replace: bool = False,
) -> Callable[[Type[C]], Type[C]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[C]) -> Type[C]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


class ConverterContext(RegistratorContext[Converter], Configurator):
    SECTION: str = "converters"

    @property
    def _registry(self) -> Registry[Converter]:
        return registry

    # noinspection PyTypeChecker
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        converter_dirs = configs.dirs.to_dict()
        converter_dirs["conf_dir"] = configs.dirs.conf.joinpath(f"{self.SECTION}.d")
        converter_generics = [c.type for c in registry.types.values() if issubclass(c.type, GenericConverter)]
        for converter in converter_generics:
            self._configure(converter, **converter_dirs)
        self._load(self, configs)

    # noinspection PyTypeChecker
    def _configure(self, cls: Type[GenericConverter], **kwargs) -> None:
        key = cls.dtype.__name__.lower()
        configs = Configurations.load(f"{key}.conf", require=False, **kwargs)
        self._add(cls(context=self, configs=configs, key=key))

    def _load(
        self,
        context: Registrator | RegistratorContext,
        configs: Configurations,
    ) -> None:
        defaults = {}
        configs = configs.copy()
        if configs.has_section(self.SECTION):
            converters = configs.get_section(self.SECTION)
            for section in self._get_type().SECTIONS:
                if section in converters:
                    defaults.update(converters.pop(section))

            self._load_sections(context, converters, defaults)
        self._load_from_file(context, configs.dirs, "converters.conf", defaults)

    def has_dtype(self, *dtypes: Type) -> bool:
        return len(self._get_by_dtypes(*dtypes)) > 0

    def get_by_dtype(self, dtype: Type) -> Converter:
        converters = self._get_by_dtypes(dtype)
        if len(converters) == 0:
            raise ResourceException(f"Converter instance for '{dtype}' does not exist")
        return converters[0]

    def _get_by_dtypes(self, dtype: Type) -> List[Converter]:
        return [t for t in self.values() if issubclass(t.dtype, dtype)]
