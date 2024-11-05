# -*- coding: utf-8 -*-
"""
lori.converters.context
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Callable, List, Optional, Type, TypeVar, overload

from lori import ResourceException
from lori.converters.converter import (
    BoolConverter,
    Converter,
    DatetimeConverter,
    FloatConverter,
    IntConverter,
    StringConverter,
    TimestampConverter,
)
from lori.core import Context, Registrator, RegistratorContext, Registry
from lori.core.configs import ConfigurationException, Configurations, Configurator

C = TypeVar("C", bound=Converter)

registry = Registry[Converter]()
registry.register(DatetimeConverter)
registry.register(TimestampConverter)
registry.register(StringConverter, "string")
registry.register(FloatConverter)
registry.register(IntConverter, "integer")
registry.register(BoolConverter, "boolean")


@overload
def register_converter_type(cls: Type[C]) -> Type[C]: ...


@overload
def register_converter_type(
    *alias: Optional[str],
    factory: Callable[..., Type[C]] = None,
    replace: bool = False,
) -> Type[C]: ...


def register_converter_type(
    *args: Optional[Type[C], str],
    **kwargs,
) -> Type[C] | Callable[[Type[C]], Type[C]]:
    args = list(args)
    if len(args) > 0 and isinstance(args[0], type):
        cls = args.pop(0)
        registry.register(cls, *args, **kwargs)
        return cls

    # noinspection PyShadowingNames
    def _register(cls: Type[C]) -> Type[C]:
        registry.register(cls, *args, **kwargs)
        return cls

    return _register


class ConverterContext(RegistratorContext[Converter], Configurator):
    SECTION: str = "converters"

    def __init__(self, context: Context, *args, **kwargs) -> None:
        from lori.data.context import DataContext

        if context is None or not isinstance(context, DataContext):
            raise ConfigurationException(f"Invalid data context: {None if context is None else type(context)}")
        super().__init__(context, *args, **kwargs)

    @property
    def _registry(self) -> Registry[Converter]:
        return registry

    # noinspection PyTypeChecker
    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        converter_dirs = configs.dirs.to_dict()
        converter_dirs["conf_dir"] = configs.dirs.conf.joinpath(self.SECTION)
        for c in [DatetimeConverter, TimestampConverter, StringConverter, FloatConverter, IntConverter, BoolConverter]:
            self._configure_converter(c, **converter_dirs)
        self._load(self, configs)

    # noinspection PyTypeChecker
    def _configure_converter(self, cls: Type[Converter], **kwargs) -> None:
        self._add(cls(key=cls.TYPE, configs=Configurations.load(f"{cls.TYPE}.conf", require=False, **kwargs)))

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
