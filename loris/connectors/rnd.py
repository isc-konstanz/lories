# -*- coding: utf-8 -*-
"""
loris.connectors.rnd
~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import random

import datetime as dt
from typing import Optional

import pandas as pd
import pytz as tz
from loris.connectors import Connector, register_connector_type
from loris.core import Resources, Resource


# noinspection PyShadowingBuiltins
@register_connector_type
class Randomizer(Connector):
    TYPE: str = "random"

    _data: pd.Series

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        self._data = pd.Series(index=[r.id for r in resources])

    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp, dt.datetime] = None,
        end: Optional[pd.Timestamp, dt.datetime] = None,
    ) -> pd.DataFrame:
        for resource in resources:
            if pd.isna(self._data[resource.id]):
                value = random.randrange(int(resource.min*100), int(resource.max*100))/100.
            else:
                range = abs(resource.max - resource.min)
                value = random.randrange(int(-range*100), int(range*100))/10. + self._data[resource.id]
                if value <= resource.min:
                    value = resource.min
                if value >= resource.max:
                    value = resource.max
            self._data[resource.id] = value
        return self._data.to_frame(pd.Timestamp.now(tz=tz.UTC)).T

    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError()
