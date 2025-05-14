# -*- coding: utf-8 -*-
"""
lori.simulation.report.core
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import Optional, Type, TypeVar

from lori.simulation import Results
from lori.core import Configurations, ResourceException
from lori.core.configs.configurator import Configurator, ConfiguratorMeta
from lori.core.register import Registry


# noinspection PyShadowingBuiltins
def register_report_type(
    type: str,
    *alias: str,
    factory: Callable[[Optional[Configurations]], ReportType] = None,
    replace: bool = False,
) -> Callable[[Type[ReportType]], Type[ReportType]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[ReportType]) -> Type[ReportType]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


class ReportMeta(ConfiguratorMeta):
    def __call__(cls, configs: Configurations, **kwargs) -> ReportType:
        _type = configs.get("type", default="default").lower()
        _cls = cls._get_class(_type)
        if cls != _cls:
            return _cls(configs, **kwargs)

        return super().__call__(configs, **kwargs)

    # noinspection PyMethodMayBeStatic, PyShadowingBuiltins
    def _get_class(cls: Type[ReportType], type: str) -> Type[ReportType]:
        if registry.has_type(type):
            registration = registry.from_type(type)
            return registration.type

        raise ResourceException(f"Unknown report type '{type}'")


class Report(Configurator, metaclass=ReportMeta):
    def __init__(self, configs: Configurations, **kwargs) -> None:
        super().__init__(configs, **kwargs)
        self.configure(configs)

    @abstractmethod
    def write(self, results: Results) -> None: ...


class ReportException(ResourceException):
    """
    Raise if an error occurred writing the report.

    """


ReportType = TypeVar("ReportType", bound=Report)

registry = Registry[Report]()
