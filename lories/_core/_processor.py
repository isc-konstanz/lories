# -*- coding: utf-8 -*-
"""
lories._core._processor
~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import Any, Collection, Dict, TypeAlias, TypeVar

from lories._core._context import _Context
from lories._core._entity import _Entity
from lories._core.typing import Timestamp


class _Processor(_Entity, Callable[[Timestamp, Any], Any]):
    INCLUDES: Collection[str] = ()
    TYPE: str = "processor"

    def __call__(self, timestamp: Timestamp, value: Any) -> Any: ...

    @abstractmethod
    def process(self, timestamp: Timestamp, value: Any, **kwargs) -> Any: ...

    @abstractmethod
    def to_configs(self) -> Dict[str, Any]: ...


Processor = TypeVar("Processor", bound=_Processor)


# noinspection PyAbstractClass
class _ProcessorContext(_Context[Processor]):
    TYPE: str = "processors"


ProcessorContext = TypeVar(
    name="ProcessorContext",
    bound=_ProcessorContext,
)
Processors: TypeAlias = _ProcessorContext
