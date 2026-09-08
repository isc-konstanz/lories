# -*- coding: utf-8 -*-
"""
lories.data.converters.linear
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any, Collection, Optional

from lories._core import _Channel  # noqa
from lories.core import ConfigurationError
from lories.core.typing import Configurations
from lories.data.converters.converter import FloatConverter


class LinearConverter(FloatConverter):
    """
    Applies ``y = scale * x + offset`` and yields a float.

    One ``linear`` instance always exists with ``scale = 1`` and ``offset = 0``; a channel sets
    both in its converter table (``converter = { type = "linear", scale = -1000.0 }``). A named
    instance declared in ``converters.d/<key>.conf`` with ``type = "linear"`` carries its own
    defaults, which a channel's table keys override.
    """

    ARGUMENTS: Collection[str] = (*FloatConverter.ARGUMENTS, "scale", "offset")

    _scale: float = 1.0
    _offset: float = 0.0

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        self._scale = configs.get_float("scale", default=1.0)
        self._offset = configs.get_float("offset", default=0.0)

    # noinspection PyUnresolvedReferences
    def assert_channel(self, channel: _Channel) -> None:
        # The result is a float by construction; an int channel would round it silently in the
        # database column, so the mismatch fails loudly instead.
        if not issubclass(channel.type, float):
            raise ConfigurationError(
                f"Converter '{self.key}' yields float values, but channel '{channel.id}' is declared as "
                f"type '{channel.type.__name__}'. Declare the channel with type = \"float\"."
            )

    def convert(
        self,
        value: Any,
        scale: Optional[float] = None,
        offset: Optional[float] = None,
        **kwargs,
    ) -> Optional[float]:
        if value is None:
            return None
        if scale is None:
            scale = self._scale
        if offset is None:
            offset = self._offset
        return float(value) * float(scale) + float(offset)
