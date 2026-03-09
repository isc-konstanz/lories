# -*- coding: utf-8 -*-
"""
lories.data.filters.filter
~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any

import pandas as pd
from lories.core.typing import Timestamp
from lories.data.processors import Processor


class Filter(Processor):
    timestamp: Timestamp = pd.NaT

    def process(self, timestamp: Timestamp, value: Any) -> Any:
        try:
            return self.filter(timestamp, value)
        finally:
            self.timestamp = timestamp

    @abstractmethod
    def filter(self, timestamp: Timestamp, value: Any) -> Any: ...
