# -*- coding: utf-8 -*-
"""
lories.data.channels.converter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd
from lories._core._channel import Channel  # noqa
from lories._core._converter import Converter, _Converter  # noqa
from lories.core import ConfigurationError
from lories.core.errors import ResourceError, ResourceUnavailableError
from lories.data.channels._core import _ChannelWrapper


class ChannelConverter(_ChannelWrapper[Converter]):
    # Inside a channel's converter table, `type` selects the converter like it does for every
    # other registrator; `converter` remains accepted as the canonical key.
    TYPE_ALIAS: str = "type"

    _channel: Channel

    # noinspection PyProtectedMember, PyUnresolvedReferences, PyTypeChecker
    @classmethod
    def build(cls, channel: Channel, **configs) -> ChannelConverter:
        converter_context = channel._context._converters
        if converter_context is None:
            raise ResourceUnavailableError(f"Missing converter context for channel '{channel.id}'")
        converter_id = cls._pop_converter_id(channel, configs)
        if converter_id is None:
            converter = converter_context.get_by_dtype(channel.type)
        else:
            converter = cls._build_registrator(converter_context, channel.path, converter_id)
            if converter is None:
                raise ConfigurationError(f"Unknown converter '{converter_id}' for channel '{channel.id}'")
        converter = cls._assert_registrator(converter)
        cls._assert_arguments(channel, converter, configs)
        converter.assert_channel(channel)
        channel_converter = cls(_Converter, converter, **configs)
        channel_converter._channel = channel
        return channel_converter

    @classmethod
    def _pop_converter_id(cls, channel: Channel, configs: Dict[str, Any]) -> Optional[str]:
        converter_id = configs.pop(_Converter.TYPE, None)
        converter_alias = configs.pop(cls.TYPE_ALIAS, None)
        if converter_id is not None and converter_alias is not None and converter_id != converter_alias:
            raise ConfigurationError(
                f"Conflicting converter selection for channel '{channel.id}': "
                f"'{_Converter.TYPE}' = '{converter_id}' and '{cls.TYPE_ALIAS}' = '{converter_alias}'"
            )
        return converter_id if converter_id is not None else converter_alias

    @classmethod
    def _assert_arguments(cls, channel: Channel, converter: Converter, configs: Dict[str, Any]) -> None:
        unknown = [k for k in configs.keys() if k != "enabled" and k not in converter.ARGUMENTS]
        if len(unknown) > 0:
            accepted = ", ".join(converter.ARGUMENTS) if len(converter.ARGUMENTS) > 0 else "none"
            raise ConfigurationError(
                f"Unknown converter argument(s) {unknown} for channel '{channel.id}' "
                f"(converter '{converter.key}' accepts: {accepted})"
            )

    def _update(self, enabled: Optional[str | bool] = None, **configs: Any) -> None:
        configs.pop(self.TYPE_ALIAS, None)
        self._assert_arguments(self._channel, self._converter, configs)
        super()._update(enabled=enabled, **configs)

    @classmethod
    def _assert_registrator(cls, converter) -> Converter:
        if converter is None or not isinstance(converter, _Converter):
            raise ResourceError(f"Invalid converter: {None if converter is None else type(converter)}")
        return converter

    @property
    def _converter(self) -> Converter:
        return self._get_registrator()

    def __call__(self, data: Any) -> Any:
        # Pushed values take the same path as a read frame (`from_series`), so a converter's
        # per-channel arguments apply whether a connector polls or pushes.
        if isinstance(data, pd.Series):
            return data.apply(self._converter.from_value, args=(self._channel,))
        return self._converter.from_value(data, self._channel)

    def to_str(self, value: Any) -> str:
        return self._converter.to_str(value)

    def to_json(self, value: Any) -> str:
        return self._converter.to_json(value)

    def to_series(self, value: Any, timestamp: Optional[pd.Timestamp] = None, name: Optional[str] = None) -> pd.Series:
        return self._converter.to_series(value, timestamp=timestamp, name=name)
