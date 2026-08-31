# -*- coding: utf-8 -*-
"""
lories.data.processors.size
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from typing import Any

from lories.core.typing import Timestamp
from lories.data.processors import Processor, register_processor_type


@register_processor_type("size", "min_size")
class SizeFilter(Processor):
    """
    Drops byte payloads smaller than ``min_size`` bytes.

    Meant for encoded frames: at a fixed resolution and JPEG quality the byte count
    tracks scene detail, so a dark, fogged or shutter-covered frame compresses to a
    fraction of a normal one. A skipped update leaves the channel's last value in
    place and is neither logged nor delivered to listeners. Values that are not
    bytes pass through untouched.
    """

    TYPE: str = "size"

    def process(self, timestamp: Timestamp, value: Any, min_size: int = 0, **kwargs: Any) -> Any:
        if not isinstance(value, (bytes, bytearray)):
            return value
        size = len(value)
        min_size = int(min_size)
        if size < min_size:
            self._logger.info("Skipping '%s' payload of %d bytes: below min_size of %d", self.id, size, min_size)
            return Processor.SKIP
        return value
