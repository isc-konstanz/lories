# -*- coding: utf-8 -*-
"""
lories.data.channels.processors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

from entsoe.parsers import parse_installed_capacity_per_plant

from lories._core._channel import Channel, _Channel  # noqa
from lories._core._processor import Processor, _Processor  # noqa
from lories.core.errors import ResourceError
from lories.core.typing import Timestamp


class ChannelProcessors(Sequence[Processor]):
    _processors: List[_Processor]

    # noinspection PyProtectedMember, PyUnresolvedReferences, PyTypeChecker
    @classmethod
    def build(cls, channel: Channel, configs: Optional[Dict[str, Any] | str] = None) -> ChannelProcessors:
        if configs is None:
            return cls()

        processors = []
        try:
            context = channel._context.__getattribute__(f"{_Processor.TYPE}s")
            for key, processor in configs.items():
                processor = _Channel._build_member(processor, "processor")
                processors.append(context._create(id=f"{channel.id}.{key}", key=key, **processor))

        except AttributeError:
            raise ResourceUnavailableError(f"Missing {_Processor.TYPE} context for channel '{channel.id}'")
        return cls(processors)

    def __init__(self, processors: Sequence[_Processor] = ()) -> None:
        self._processors = [self._assert_processor(p) for p in processors]

    @classmethod
    def _assert_processor(cls, processor: Processor) -> Optional[Processor]:
        if processor is None:
            return None
        if not isinstance(processor, _Processor):
            raise ResourceError(f"Invalid processor: {None if processor is None else type(processor)}")
        return processor

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, ChannelProcessors)
            and self._processors == other._processors
        )

    def __hash__(self) -> int:
        return hash(self._processors)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(str(p.id) for p in self._processors)})"

    def __str__(self) -> str:
        return f"{type(self).__name__}:\n\t" + "\n\t".join(f"{p.id} = {repr(p)}" for p in self._processors)

    def __contains__(self, processor: str | Processor) -> bool:
        if isinstance(processor, str):
            return any(processor == p.id for p in self._processors)
        return processor in self._processors

    def __getitem__(self, index: Iterable[str] | str | int) -> Processor | ChannelProcessors:
        if isinstance(index, str):
            for processor in self._processors:
                if processor.id == index:
                    return processor
        if isinstance(index, Iterable):
            return type(self)([p for p in self._processors if p.id == index])
        raise KeyError(index)

    def __iter__(self) -> Iterator[Processor]:
        return iter(self._processors)

    def __len__(self) -> int:
        return len(self._processors)

    def __add__(self, other) -> ChannelProcessors:
        return type(self)([*self, *other])

    @property
    def ids(self) -> Sequence[str]:
        return [str(processor.id) for processor in self._processors]

    @property
    def keys(self) -> Sequence[str]:
        return [str(processor.key) for processor in self._processors]

    def to_configs(self) -> Dict[str, Any]:
        return { p.key: dict(p.to_configs()) for p in self._processors }

    def append(self, processor: Processor) -> None:
        self._processors.append(processor)

    def extend(self, processors: Iterable[Processor]) -> None:
        self._processors.extend(processors)

    def update(self, processors: Iterable[Processor]) -> None:
        processor_ids = [r.id for r in processors]
        for processor in [r for r in self._processors if r.id in processor_ids]:
            self._processors.remove(processor)
        self._processors.extend(processors)

    def copy(self) -> ChannelProcessors:
        return type(self)([processor.copy() for processor in self._processors])

    def process(self, timestamp: Timestamp, value: Any) -> Any:
        for processor in self._processors:
            value = processor.process(timestamp, value)
        return value
