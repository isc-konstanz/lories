# -*- coding: utf-8 -*-
"""
lories.data.channels.converter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from lories._core._channel import Channel  # noqa
from lories._core._converter import Converter, _Converter  # noqa
from lories.core.errors import ResourceError, ResourceUnavailableError
from lories.data.channels._wrapper import _ChannelWrapper


class ChannelConverter(_ChannelWrapper[Converter]):

    # noinspection PyProtectedMember, PyUnresolvedReferences, PyTypeChecker
    @classmethod
    def build(cls, channel: Channel, **configs) -> ChannelConverter:
        converter_context =  channel._context._converters
        if converter_context is None:
            raise ResourceUnavailableError(f"Missing converter context for channel '{channel.id}'")
        converter_id = configs.pop(_Converter.TYPE, None)
        if converter_id is None:
            converter = converter_context.get_by_dtype(channel.type)
        else:
            converter = cls._build_registrator(converter_context, channel.path, converter_id)
        return cls(_Converter, converter, **configs)

    @classmethod
    def _assert_registrator(cls, converter) -> Converter:
        if converter is None or not isinstance(converter, _Converter):
            raise ResourceError(f"Invalid converter: {None if converter is None else type(converter)}")
        return converter

    @property
    def _converter(self) -> Converter:
        return self._get_registrator()

    def __call__(self, data: Any) -> Any:
        converter_args = self._get_configs()
        if isinstance(data, pd.Series):
            converted_data = data.apply(self._converter.convert, **converter_args)
            return converted_data.apply(self._converter.to_dtype, **converter_args)
        converted_data = self._converter.convert(data, **converter_args)
        return self._converter.to_dtype(converted_data, **converter_args)

    def to_str(self, value: Any) -> str:
        return self._converter.to_str(value)

    def to_json(self, value: Any) -> str:
        return self._converter.to_json(value)

    def to_series(self, value: Any, timestamp: Optional[pd.Timestamp] = None, name: Optional[str] = None) -> pd.Series:
        return self._converter.to_series(value, timestamp=timestamp, name=name)
