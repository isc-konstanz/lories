# -*- coding: utf-8 -*-
"""
lori.components.tariff.core
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import Callable
from typing import Optional, Type, TypeVar

from lori.components import Component, register_component_type
from lori.core import Configurations, Constant, Context, ResourceException, ResourceUnavailableException
from lori.core.activator import ActivatorMeta
from lori.core.register import Registrator, Registry
from lori.location import Location, LocationUnavailableException


# noinspection PyShadowingBuiltins
def register_tariff_type(
    type: str,
    *alias: str,
    factory: Callable[[Context | Registrator, Optional[Configurations]], TariffType] = None,
    replace: bool = False,
) -> Callable[[Type[TariffType]], Type[TariffType]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[TariffType]) -> Type[TariffType]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


class TariffMeta(ActivatorMeta):
    def __call__(cls, context: Context | Component, configs: Configurations, **kwargs) -> Tariff:
        _type = configs.get("type", default="default").lower()
        _cls = cls._get_class(_type)
        if cls != _cls:
            return _cls(context, configs, **kwargs)

        return super().__call__(context, configs, **kwargs)

    # noinspection PyMethodMayBeStatic, PyShadowingBuiltins
    def _get_class(cls: Type[TariffType], type: str) -> Type[TariffType]:
        if type in ["default", "virtual"]:
            return cls
        elif registry.has_type(type):
            registration = registry.from_type(type)
            return registration.type

        raise TariffException(f"Unknown tariff type '{type}'")


# noinspection SpellCheckingInspection
@register_component_type("tariff")
class Tariff(Component, metaclass=TariffMeta):
    IMPORT = Constant(float, "import", name="Import Tariff", unit="ct/kWh")
    EXPORT = Constant(float, "export", name="Export Tariff", unit="ct/kWh")


class TariffException(ResourceException):
    """
    Raise if an error occurred accessing the tariff.

    """


class TariffUnavailableException(ResourceUnavailableException, TariffException):
    """
    Raise if a configured tariff access can not be found.

    """


TariffType = TypeVar("TariffType", bound=Tariff)

registry = Registry[Tariff]()
