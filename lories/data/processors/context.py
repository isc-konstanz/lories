# -*- coding: utf-8 -*-
"""
lories.data.processors.context
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Type

from lories._core._processor import Processor, _Processor, _ProcessorContext  # noqa
from lories._core._tasks import TaskContext, _TaskContext  # noqa
from lories.core import ResourceError
from lories.core.register import Registry

registry = Registry[_Processor]()  # noqa


# noinspection PyShadowingBuiltins
def register_processor_type(
    type: str,
    *alias: str,
    factory: Callable[..., Processor] = None,
    replace: bool = False,
) -> Callable[[Type[Processor]], Type[Processor]]:
    # noinspection PyShadowingNames
    def _register(cls: Type[Processor]) -> Type[Processor]:
        registry.register(cls, type, *alias, factory=factory, replace=replace)
        return cls

    return _register


# noinspection PyShadowingBuiltins
class ProcessorContext(_ProcessorContext):
    # noinspection PyMethodMayBeStatic
    def _create(
        self,
        id: str,
        key: str,
        processor: str,
        **configs: Any,
    ) -> Processor:
        registration = registry.from_type(processor)
        return registration.initialize(id=id, key=key, _type=registration.key, **configs)

    # noinspection PyProtectedMember
    def _update(
        self,
        id: str,
        key: str,
        processor: str,
        **configs: Any,
    ) -> None:
        _processor = self._get(id)
        if len(configs) > 0:
            raise ResourceError(f"Unable to update processor '{_processor.id}' for fields: {', '.join(configs.keys())}")
