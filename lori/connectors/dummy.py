# -*- coding: utf-8 -*-
"""
lori.connectors.dummy
~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import datetime as dt
import random
from typing import Optional

import pandas as pd
import pytz as tz
from lori import ConfigurationException
from lori.connectors import Connector, register_connector_type
from lori.core import Resources


# noinspection PyShadowingBuiltins
@register_connector_type("dummy", "random")
class DummyConnector(Connector):
    _data: pd.Series

    def connect(self, resources: Resources) -> None:
        super().connect(resources)
        self._data = pd.Series(index=[r.id for r in resources])
        for resource in resources:
            for attr in ["min", "max"]:
                if attr not in resource:
                    raise ConfigurationException(f"Invalid dummy channel, missing '{attr}' attribute: {resource.id}")

    def read(
        self,
        resources: Resources,
        start: Optional[pd.Timestamp | dt.datetime] = None,
        end: Optional[pd.Timestamp | dt.datetime] = None,
    ) -> pd.DataFrame:
        for resource in resources:
            if pd.isna(self._data[resource.id]):
                value = float(random.randrange(int(resource.min * 100), int(resource.max * 100))) / 100.0
            else:
                range = int(abs(resource.max - resource.min))
                value = float(random.randrange(-range * 100, range * 100)) / 1000.0 + self._data[resource.id]
                if value < resource.min:
                    value = resource.min
                if value > resource.max:
                    value = resource.max
            self._data[resource.id] = value
        return self._data.to_frame(pd.Timestamp.now(tz.UTC).floor(freq="s")).T

    def write(self, data: pd.DataFrame) -> None:
        for id in data.columns:
            channel = self.channels[id]
            value = data.loc[data.index[-1], id]
            if value < channel.min:
                value = channel.min
            if value > channel.max:
                value = channel.max
            self._data[id] = value
