# -*- coding: utf-8 -*-
"""
lori.components.tariff.tariff
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import Callable
from typing import Optional, Type, TypeVar

from lori.components import Component, register_component_type
from lori.core import Constant, ResourceError
from lori.core.activator import ActivatorMeta
from lori.core.register import Registry
from lori.typing import Configurations, ContextArgument
from lori.util import validate_key


# noinspection PyShadowingBuiltins
def register_tariff_type(
    type: str,
    *alias: str,
    factory: Callable[[ContextArgument, Optional[Configurations]], TariffType] = None,
    replace: bool = False,
) -> Callable[[Type[TariffType]], Type[TariffType]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[TariffType]) -> Type[TariffType]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


class TariffMeta(ActivatorMeta):
    def __call__(cls, context: ContextArgument, configs: Configurations, **kwargs) -> TariffType:
        _type = validate_key(configs.get("type", default="default"))
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

        raise ResourceError(f"Unknown tariff type '{type}'")


# noinspection SpellCheckingInspection
@register_component_type("tariff")
class Tariff(Component, metaclass=TariffMeta):
    PRICE_IMPORT = Constant(float, "price_import", name="Import Tariff Price", unit="ct/kWh")
    PRICE_EXPORT = Constant(float, "price_export", name="Export Tariff Price", unit="ct/kWh")


TariffType = TypeVar("TariffType", bound=Tariff)

registry = Registry[Tariff]()
